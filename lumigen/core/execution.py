from __future__ import annotations
import typing as t
from collections import OrderedDict
import logging
from sqlglot import exp
from sqlmesh.core.engine_adapter.base import (
    EngineAdapter, MERGE_SOURCE_ALIAS, MERGE_TARGET_ALIAS
)

if t.TYPE_CHECKING:
    from core.job_context import JobContext

from utils import merge_dicts, FallbackSet
from utils.dataset import columns_to_types_from_df, get_columns_name
from utils.parsing import dialect_insensitive_tbl_name
from core.config.job import TransformationConfig, DataSaveMode
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

def write_data(dataset, t_chain: TransformationPipeline, context: JobContext):
    writer = MicroBatchWriter(context.sink_adapter)
    sink_table = context.job_props_model.get_sink_path()
    sink_table = dialect_insensitive_tbl_name(sink_table, writer.dialect)

    data_save_mode = context.job_props_model.data_save_mode
    if data_save_mode == DataSaveMode.OVERWRITE:
        t_chain.add(writer.replace_query, [sink_table],
                    [], context.job_props_model.replace_query_kwargs or {})
    elif data_save_mode == DataSaveMode.APPEND:
        t_chain.add(writer.insert_append, [sink_table])
    elif data_save_mode == DataSaveMode.MERGE:
        t_chain.add(writer.merge, [sink_table],
                    [context.job_props_model.sink_key],
                    {"when_matched_filter": context.job_props_model.when_matched_filter})
    elif data_save_mode == DataSaveMode.ERROR_IF_EXISTS:
        raise NotImplementedError(f"{DataSaveMode.ERROR_IF_EXISTS} not implemented.") # TODO
    else:
        raise NotImplementedError(f"{DataSaveMode.IGNORE} not implemented.") # TODO

    for df in dataset:
        t_chain.apply(df)

class MicroBatchWriter:
    def __init__(self, base_writer: EngineAdapter):
        self.base = base_writer
        self.dialect = self.base.dialect
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
                tbl_col_set = FallbackSet(table_columns)

                # Must match the order of columns in the data frame to map correctly
                self._columns_to_types = OrderedDict()
                df_columns = get_columns_name(df)
                for df_col in df_columns:
                    tbl_col = tbl_col_set.get(df_col)
                    if tbl_col in table_columns:
                        self._columns_to_types[tbl_col] = table_columns[tbl_col]
            else:
                self._columns_to_types = columns_to_types_from_df(df)
        return self._columns_to_types