from __future__ import annotations
import typing as t
import logging
import os
import re
import pandas as pd
import threading
import queue
from concurrent.futures import ThreadPoolExecutor

if t.TYPE_CHECKING:
    from fsspec import AbstractFileSystem

from engines.fs.request_parser import FileRequest, FileRequestParser, FileOperationType, FileOptions, RequestParseError
from engines.fs.handlers import FileHandlerFactory
from utils.dataset import columns_to_types_from_df

logger = logging.getLogger(__name__)


class FileSystemCursor:
    METADATA_COLUMN_PREFIX = "_metadata_"

    def __init__(self, fs: AbstractFileSystem):
        self._fs = fs
        self._last_df: t.Optional[pd.DataFrame] = None
        self._row_iterator: t.Optional[t.Iterator[t.Tuple]] = None

    def close(self) -> None:
        pass

    @staticmethod
    def _clean_path(path: str) -> str:
        if not path:
            return path
        # Strip hdfs://host:port/ or hdfs:/// scheme
        if path.startswith("hdfs://"):
            cleaned = re.sub(r'^hdfs://(?:[^/]+)?', '', path)
            return cleaned if cleaned else "/"
        # Strip s3://, s3a://, s3n:// schemes
        return re.sub(r'^s3[a-z]?://', '', path)

    def _infer_unified_schema(self, files: t.Dict[str, t.Dict[str, t.Any]], parsed_request: FileRequest) -> pd.DataFrame:
        """
        Quickly scans the headers/metadata of all matched files in parallel and merges them to determine the global, unified schema.
        Returns an empty DataFrame containing only the resolved columns and datatypes.
        """
        df_list = []
        with ThreadPoolExecutor(max_workers=parsed_request.concurrency) as executor:
            futures = [
                executor.submit(self._describe_single_file, path, info, parsed_request.options) 
                for path, info in files.items()
            ]
            for future in futures:
                try:
                    res = future.result()
                    if res is not None:
                        df_list.append(res)
                except Exception as e:
                    logger.warning(f"Error reading schema for file in thread: {e}")

        if not df_list:
            return pd.DataFrame()
            
        combined_df: pd.DataFrame = pd.concat(df_list, ignore_index=True)
        return combined_df.iloc[0:0]  # Return 0 rows, strictly for schema retention

    def execute(self, request: str, payload: t.Optional[t.Any] = None, **kwargs) -> None:
        self._last_df = None
        self._row_iterator = None

        try:
            parsed_request = FileRequestParser.parse(request)
        except RequestParseError as e:
            logger.error(f"Failed to execute request due to parsing error.", exc_info=e)
            raise

        if parsed_request.operation == FileOperationType.READ:
            target_path = self._clean_path(parsed_request.file_pattern or parsed_request.file_path)
            files: t.Dict[str, t.Dict[str, t.Any]] = self._fs.glob(target_path, detail=True)
            if parsed_request.modified_since is not None:
                files = {
                    path: info for path, info in files.items()
                    if info.get("LastModified", info.get("mtime")) is not None 
                    and info.get("LastModified", info.get("mtime")) > parsed_request.modified_since
                }
            if not files:
                return

            # Resolve Global Schema before streaming begins
            unified_schema_df = self._infer_unified_schema(files, parsed_request)
            if len(unified_schema_df.columns) == 0:
                return

            # Instantly populates .description properly
            self._last_df = unified_schema_df
            chunk_stream = self._generate_read_chunks(files, parsed_request, unified_schema_df)

            def row_generator():
                for chunk in chunk_stream:
                    yield from chunk.itertuples(index=False, name=None)
            self._row_iterator = row_generator()
        elif parsed_request.operation == FileOperationType.DESCRIBE:
            target_path = self._clean_path(parsed_request.file_pattern or parsed_request.file_path)
            files: t.Dict[str, t.Dict[str, t.Any]] = self._fs.glob(target_path, detail=True)
            if parsed_request.modified_since is not None:
                files = {
                    path: info for path, info in files.items()
                    if info.get("LastModified", info.get("mtime")) is not None 
                    and info.get("LastModified", info.get("mtime")) > parsed_request.modified_since
                }
            if not files:
                self._last_df = pd.DataFrame(columns=["field", "type"])
                return

            unified_schema_df = self._infer_unified_schema(files, parsed_request)
            if len(unified_schema_df.columns) > 0:
                self._last_df = pd.DataFrame(
                    {"field": name, "type": dtype.sql()}
                    for name, dtype in columns_to_types_from_df(unified_schema_df).items()
                )
            else:
                self._last_df = pd.DataFrame(columns=["field", "type"])
            self._row_iterator = self._last_df.itertuples(index=False, name=None)
        elif parsed_request.operation == FileOperationType.WRITE:
            if isinstance(payload, pd.DataFrame):
                df = payload
            else:
                raise ValueError(f"Unsupported payload type: {type(payload)}.")
            
            cleaned_path = self._clean_path(parsed_request.file_path)
            self._write_single_file(df, cleaned_path, parsed_request.options)
        elif parsed_request.operation == FileOperationType.APPEND:
            if isinstance(payload, pd.DataFrame):
                df = payload
            else:
                raise ValueError(f"Unsupported payload type: {type(payload)}.")
            
            cleaned_path = self._clean_path(parsed_request.file_path)
            self._append_single_file(df, cleaned_path, parsed_request.options)
        elif parsed_request.operation == FileOperationType.REMOVE:
            if parsed_request.file_path:
                cleaned_path = self._clean_path(parsed_request.file_path)
                if self._fs.exists(cleaned_path):
                    self._fs.rm(cleaned_path, recursive=True)
            if parsed_request.file_pattern:
                cleaned_path = self._clean_path(parsed_request.file_pattern)
                files_to_remove = self._fs.glob(cleaned_path)
                if files_to_remove:
                    try:
                        self._fs.rm(files_to_remove, recursive=True)
                    except:
                        for file_path in files_to_remove:
                            self._fs.rm(file_path, recursive=True)
        elif parsed_request.operation == FileOperationType.LIST:
            target_path = self._clean_path(parsed_request.file_pattern or parsed_request.file_path)
            files: t.Dict[str, t.Dict[str, t.Any]] = self._fs.glob(target_path, detail=True)
            # Only list supported file extensions
            files = {
                path: info for path, info in files.items()
                if FileHandlerFactory.get_handler(os.path.splitext(path)[-1]) is not None
            }
            if parsed_request.modified_since is not None:
                files = {
                    path: info for path, info in files.items()
                    if info.get("LastModified", info.get("mtime")) is not None 
                    and info.get("LastModified", info.get("mtime")) > parsed_request.modified_since
                }
            if files:
                self._last_df = pd.DataFrame(
                    {
                        "file_path": path,
                        "file_name": os.path.basename(path),
                        "file_size": info.get("Size", info.get("size")),
                        "file_modification_time": info.get("LastModified", info.get("mtime"))
                    }
                    for path, info in files.items()
                )
            else:
                self._last_df = pd.DataFrame(columns=["file_path", "file_name", "file_size", "file_modification_time"])
            self._row_iterator = self._last_df.itertuples(index=False, name=None)
        else:
            raise NotImplementedError(f"Operation {parsed_request.operation} is not implemented.")

    def _generate_read_chunks(self, files: t.Dict[str, t.Any], parsed_request: FileRequest, unified_schema_df: pd.DataFrame) -> t.Iterator[pd.DataFrame]:
        # Bounding the queue forces background threads to pause reading if the consumer gets too slow.
        chunk_queue = queue.Queue(maxsize=parsed_request.concurrency * 2)
        master_columns = list(unified_schema_df.columns)
        stop_event = threading.Event()

        def worker(path, info):
            try:
                for chunk_df in self._read_file_chunks(path, info, parsed_request.options):
                    if stop_event.is_set():
                        return
                    # Missing columns become NaN. Unwanted extra columns are dropped.
                    aligned_df = chunk_df.reindex(columns=master_columns)
                    while not stop_event.is_set():
                        try:
                            chunk_queue.put(("data", aligned_df), timeout=1.0)
                            break
                        except queue.Full:
                            continue
            except Exception as e:
                chunk_queue.put(("error", e))

        def manager():
            with ThreadPoolExecutor(max_workers=parsed_request.concurrency) as executor:
                futures = [executor.submit(worker, p, i) for p, i in files.items()]
                for f in futures:
                    try:
                        f.result()
                    except Exception as e:
                        chunk_queue.put(("error", e))
            chunk_queue.put(("done", None))

        threading.Thread(target=manager, daemon=True).start()
        try:
            while True:
                msg_type, payload = chunk_queue.get()
                if msg_type == "done":
                    break
                elif msg_type == "error":
                    raise payload
                elif msg_type == "data":
                    yield payload
        finally:
            stop_event.set()

    def _read_file_chunks(self, file_path: str, file_info: t.Dict[str, t.Any], file_options: FileOptions) -> t.Iterator[pd.DataFrame]:
        _, file_ext = os.path.splitext(file_path.lower())
        handler = FileHandlerFactory.get_handler(file_ext)
        if not handler:
            return 
        
        for df in handler.read(self._fs, file_path, file_options, self.METADATA_COLUMN_PREFIX):
            if df is not None and not df.empty:
                df[self.METADATA_COLUMN_PREFIX+"file_path"] = file_path
                df[self.METADATA_COLUMN_PREFIX+"file_name"] = os.path.basename(file_path)
                df[self.METADATA_COLUMN_PREFIX+"file_size"] = file_info.get("Size", file_info.get("size"))
                df[self.METADATA_COLUMN_PREFIX+"file_modification_time"] = file_info.get("LastModified", file_info.get("mtime"))
                yield df
    
    def _describe_single_file(self, file_path: str, file_info: t.Dict[str, t.Any] = {}, file_options: t.Optional[FileOptions] = None) -> t.Optional[pd.DataFrame]:
        _, file_ext = os.path.splitext(file_path.lower())
        handler = FileHandlerFactory.get_handler(file_ext)
        if not handler:
            return None

        df = handler.describe(self._fs, file_path, file_options, self.METADATA_COLUMN_PREFIX)
        if df is not None:
            df[self.METADATA_COLUMN_PREFIX+"file_path"] = file_path
            df[self.METADATA_COLUMN_PREFIX+"file_name"] = os.path.basename(file_path)
            df[self.METADATA_COLUMN_PREFIX+"file_size"] = file_info.get("Size", file_info.get("size"))
            df[self.METADATA_COLUMN_PREFIX+"file_modification_time"] = file_info.get("LastModified", file_info.get("mtime"))
        return df
    
    def _write_single_file(self, df: pd.DataFrame, file_path: str, file_options: t.Optional[FileOptions] = None) -> t.Optional[pd.DataFrame]:
        try:
            _, file_ext = os.path.splitext(file_path.lower())
            handler = FileHandlerFactory.get_handler(file_ext)
            if not handler:
                raise ValueError(f"Unsupported file format for writing: {file_path}")
            
            handler.write(self._fs, df, file_path, file_options, self.METADATA_COLUMN_PREFIX)
        except Exception as e:
            logger.error(f"Failed to write file to {file_path}.", exc_info=e)
            raise

    def _append_single_file(self, df: pd.DataFrame, file_path: str, file_options: t.Optional[FileOptions] = None) -> t.Optional[pd.DataFrame]:
        cleaned_path = self._clean_path(file_path)
        if not self._fs.exists(cleaned_path):
            return self._write_single_file(df, file_path, file_options)

        try:
            _, file_ext = os.path.splitext(file_path.lower())
            handler = FileHandlerFactory.get_handler(file_ext)
            if not handler:
                raise ValueError(f"Unsupported file format for appending: {file_path}")
            
            handler.append(self._fs, df, file_path, file_options, self.METADATA_COLUMN_PREFIX)
        except Exception as e:
            logger.error(f"Failed to append data to file {file_path}.", exc_info=e)
            raise

    def fetchdf(self) -> pd.DataFrame:
        if self._row_iterator is None or self._last_df is None:
            return pd.DataFrame()
            
        remaining_rows = list(self._row_iterator)
        if not remaining_rows:
            return self._last_df.iloc[0:0] # Return empty df with matching schema

        # Construct dataframe and map back the original datatypes
        df = pd.DataFrame(remaining_rows, columns=self._last_df.columns)
        for col in df.columns:
            try:
                df[col] = df[col].astype(self._last_df[col].dtype)
            except Exception:
                pass
        return df
        
    def fetchone(self) -> t.Optional[t.Tuple]:
        if self._row_iterator is None:
            return None
        return next(self._row_iterator, None)
    
    def fetchmany(self, size: int = 1) -> t.List[t.Tuple]:
        if self._row_iterator is None:
            return []
        
        results = []
        for _ in range(size):
            row = next(self._row_iterator, None)
            if row is None:
                break
            results.append(row)
        return results

    def fetchall(self) -> t.List[t.Tuple]:
        if self._row_iterator is None:
            return []
        return list(self._row_iterator)
    
    @property
    def description(self) -> t.Optional[t.List[t.Tuple]]:
        if self._last_df is None:
            return None
        
        return list((col_name, str(col_type), None, None, None, None, True) for col_name, col_type in zip(self._last_df.columns, self._last_df.dtypes))
    
    @property
    def rowcount(self) -> int:
        return -1 # row count is unknown until iteration finishes