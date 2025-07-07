from __future__ import annotations
import typing as t
from collections import OrderedDict
import logging
from sqlglot import exp
from sqlmesh.core.engine_adapter.base import (
    EngineAdapter, MERGE_SOURCE_ALIAS, MERGE_TARGET_ALIAS
)
from utils import merge_dicts, FallbackKeyDict
from utils.dataset import columns_to_types_from_df, get_columns_name
from core.config.job import TransformationConfig, DataSaveMode, JobSinkRelatedFields
from core.engine_adapter import dialect_insensitive_tbl_name
from transformers import transformer_factory, TransformationPipeline


def transform_data(transformation_pipeline: t.List[TransformationConfig], job_props: dict) -> TransformationPipeline:
    trans_chain = TransformationPipeline()
    for transform_cfg in transformation_pipeline:
        transformer = transformer_factory(transform_cfg.name)
        try:
            trans_chain.add_transformer(transformer, **merge_dicts(job_props, transform_cfg.options))
        except TypeError as e:
            transformer_logger = logging.getLogger(transformer.__class__.__name__)
            transformer_logger.exception(f"TypeError during transformer setup: {e}")
            raise e

    return trans_chain

def write_data(dataset, t_chain: TransformationPipeline, writer: EngineAdapter, job_props: dict):
    sink_table = JobSinkRelatedFields(**job_props).get_sink_table()
    sink_table = dialect_insensitive_tbl_name(sink_table, writer.dialect)
    writer = MicroBatchWriter(writer)

    data_save_mode = job_props.get("data_save_mode", DataSaveMode.OVERWRITE)
    if data_save_mode == DataSaveMode.OVERWRITE:
        t_chain.add(writer.replace_query, [sink_table], [], job_props.get("replace_query_kwargs", {}))
    elif data_save_mode == DataSaveMode.APPEND:
        t_chain.add(writer.insert_append, [sink_table])
    elif data_save_mode == DataSaveMode.MERGE:
        t_chain.add(writer.merge, [sink_table], [job_props["key"]], {"when_matched_filter": job_props.get("when_matched_filter")})
    elif data_save_mode == DataSaveMode.ERROR_IF_EXISTS:
        raise NotImplementedError(f"{DataSaveMode.ERROR_IF_EXISTS} not implemented.") # TODO
    else:
        raise NotImplementedError(f"{DataSaveMode.IGNORE} not implemented.") # TODO

    for df in dataset:
        t_chain.apply(df)

class MicroBatchWriter:
    def __init__(self, base_writer: EngineAdapter):
        self.base = base_writer
        self.batch_id = 0
        self._columns_to_types = None

    def replace_query(self, table_name, df, *args, **kwargs):
        if self.batch_id == 0:
            self.base.replace_query(table_name, df, columns_to_types=self.get_columns_to_types(df, table_name), *args, **kwargs)
        else:
            self.base.insert_append(table_name, df, columns_to_types=self.get_columns_to_types(df, table_name), *args)
        self.batch_id += 1

    def insert_append(self, table_name, df, *args, **kwargs):
        self.base.insert_append(table_name, df, columns_to_types=self.get_columns_to_types(df, table_name), *args, **kwargs)

    def merge(self, target_table, df, key, when_matched_filter=None, *args, **kwargs):
        unique_key = [exp.column(k) for k in key]
        if when_matched_filter:
            columns_to_types = self.base._columns_to_types(df)
            when_matched = exp.Whens()
            when_matched.append(
                "expressions",
                exp.When(
                    matched=True,
                    source=False,
                    condition=exp.condition(when_matched_filter),
                    then=exp.Update(
                        expressions=[
                            exp.column(col, MERGE_TARGET_ALIAS).eq(
                                exp.column(col, MERGE_SOURCE_ALIAS)
                            )
                            for col in columns_to_types if col not in key
                        ],
                    ),
                ),
            )
        else:
            when_matched = None

        self.base.merge(target_table, df, self.get_columns_to_types(df, target_table), unique_key, when_matched=when_matched, *args, **kwargs)

    def get_columns_to_types(self, df, table_name: str = None) -> t.Dict[str, exp.DataType]:
        if self._columns_to_types is None:
            if table_name and self.base.table_exists(table_name):
                table_columns = self.base.columns(table_name)
                col_map = FallbackKeyDict()
                for c in table_columns:
                    col_map[c] = c

                # Must match the order of columns in the data frame to map correctly
                self._columns_to_types = OrderedDict()
                df_columns = get_columns_name(df)
                for df_col in df_columns:
                    tbl_col = col_map[df_col]
                    if tbl_col in table_columns:
                        self._columns_to_types[tbl_col] = table_columns[tbl_col]
            else:
                self._columns_to_types = columns_to_types_from_df(df)
        return self._columns_to_types