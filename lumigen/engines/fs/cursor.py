from __future__ import annotations
import typing as t
import logging
import os
import re
import pandas as pd
import datetime
from glob import has_magic
from fsspec.utils import glob_translate
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
        """
        Removes any RFC 3986 compliant URI scheme (e.g., s3a://, file://, gs://).
        For HDFS, both the scheme and connection authority are removed.
        """
        if not path:
            return path
            
        # HDFS-specific authority stripping
        if path.startswith("hdfs://"):
            cleaned = re.sub(r'^hdfs://(?:[^/]+)?', '', path)
            return cleaned if cleaned else "/"
            
        # Universal scheme stripping (handles any alphanumeric scheme followed by ://)
        return re.sub(r'^[a-zA-Z][a-zA-Z0-9+.-]*://', '', path)

    @staticmethod
    def _normalize_mtime(info: dict) -> datetime.datetime:
        """
        Converts any fsspec file modification time into a standardized, timezone-aware UTC datetime.
        """
        mtime = info.get("LastModified") or info.get("mtime") or info.get("time")
        
        if mtime is None:
            return None
            
        # Timezone-aware datetime (e.g., s3fs, sftpfs)
        if isinstance(mtime, datetime.datetime) and mtime.tzinfo is not None:
            return mtime.astimezone(datetime.timezone.utc)
            
        # Timezone-naive datetime (e.g., some HDFS / PyArrow versions)
        if isinstance(mtime, datetime.datetime) and mtime.tzinfo is None:
            return mtime.replace(tzinfo=datetime.timezone.utc)
            
        # Float / Integer epoch timestamp (e.g., LocalFileSystem / os.stat)
        if isinstance(mtime, (int, float)):
            return datetime.datetime.fromtimestamp(mtime, tz=datetime.timezone.utc)
            
        # Pandas / Numpy Timestamp
        if hasattr(mtime, "to_pydatetime"):
            py_dt = mtime.to_pydatetime()
            if py_dt.tzinfo is None:
                return py_dt.replace(tzinfo=datetime.timezone.utc)
            return py_dt.astimezone(datetime.timezone.utc)
            
        return None

    def _lazy_glob(self, target_path: str, modified_since: t.Optional[t.Any] = None,
                   require_file_exists: bool = False,
                   require_supported: bool = True, strict_extension_check: bool = False) -> t.Iterator[t.Tuple[str, t.Dict[str, t.Any]]]:
        """
        Lazily evaluates glob patterns. Yields file metadata tuples one-by-one.
        Universally replaces fsspec.glob() and fsspec.exists()
        """
        pre_fetched_listing = None
        listing_fetched = False

        # Explicit extension validation
        if require_supported and strict_extension_check and not target_path.endswith("/"):
            _, ext = os.path.splitext(target_path)
            if ext and not has_magic(ext):
                ext_lower = ext.lower()
                if FileHandlerFactory.get_handler(ext_lower) is None:
                    is_dir = False
                    
                    # If the path has magic (e.g. **/*.parquet), it's not an absolute directory name.
                    # It's an explicit file search for an unsupported type. It can be rejected instantly.
                    if not has_magic(target_path):
                        # Edge Case: Absolute path might be a directory named with a dot (e.g. /data/my.folder)
                        try:
                            pre_fetched_listing = self._fs.ls(target_path, detail=True)
                            listing_fetched = True
                            clean_target_stripped = target_path.strip("/")
                            
                            exact_file = next(
                                (item for item in pre_fetched_listing if item.get("name", "").strip("/") == clean_target_stripped and item.get("type") == "file"), 
                                None
                            )
                            # If it's not an exact file, but items were returned, it's a directory
                            if exact_file is None and len(pre_fetched_listing) > 0:
                                is_dir = True
                        except (FileNotFoundError, OSError):
                            pass
                    
                    # If it's a file pattern or a missing path, raise the error.
                    if not is_dir:
                        raise ValueError(f"Explicitly requested file extension '{ext_lower}' is not supported.")

        def _matches_filter(path: str, info: t.Dict[str, t.Any]) -> bool:
            if info.get("type") != "file":
                return False
            if require_supported:
                ext = os.path.splitext(path)[-1].lower()
                if FileHandlerFactory.get_handler(ext) is None:
                    return False
            if modified_since is not None:
                mtime = self._normalize_mtime(info)
                if mtime is not None and mtime <= modified_since:
                    return False
            return True

        # SCENARIO A: Absolute Path (Single LS Query Optimization)
        if not has_magic(target_path):
            listing = None
            if listing_fetched:
                listing = pre_fetched_listing
            else:
                try:
                    # AWS S3: Missing file -> [], Exact file -> [info], Dir -> [info1, info2]
                    # HDFS: Dir -> [info1, info2], Exact File -> raises ArrowIOError "Not a directory"
                    listing = self._fs.ls(target_path, detail=True)
                except FileNotFoundError:
                    if require_file_exists and os.path.splitext(target_path)[1]:
                        raise FileNotFoundError(f"No such file: '{target_path}'")
                    return
                except OSError:
                    # Catch PyArrow's HDFS bug where ls() on a file raises an uncaught generic OSError
                    pass

            if listing is None:
                try:
                    info = self._fs.info(target_path)
                    listing = [info]
                except (FileNotFoundError, OSError):
                    return

            if not listing:
                return

            # Normalized path comparison (PyArrow strips leading slashes in paths, so we strip both sides)
            clean_target_stripped = target_path.strip("/")
            
            # Check if listing contains an exact single file match (prevents S3 prefix collision bugs)
            exact_file = next(
                (item for item in listing if item.get("name", "").strip("/") == clean_target_stripped and item.get("type") == "file"), 
                None
            )

            # Case 1: Target is an exact single existing file
            if exact_file is not None:
                if _matches_filter(target_path, exact_file):
                    yield (target_path, exact_file) # Yield exactly what the user asked for
                return

            # Case 2: Target is a directory (listing contains directory contents)
            for item in listing:
                path: str = item.get("name", "")
                if _matches_filter(path, item):
                    # Ensure leading slashes match user input
                    if target_path.startswith("/") and not path.startswith("/"):
                        path = "/" + path
                    yield (path, item)
            return

        # SCENARIO B: Glob Pattern (Lazy Traversal)
        # Identify the static root directory to start walking from
        idx_star = target_path.find("*") if target_path.find("*") >= 0 else len(target_path)
        idx_qmark = target_path.find("?") if target_path.find("?") >= 0 else len(target_path)
        idx_brace = target_path.find("[") if target_path.find("[") >= 0 else len(target_path)
        min_idx = min(idx_star, idx_qmark, idx_brace)

        if "/" in target_path[:min_idx]:
            min_idx = target_path[:min_idx].rindex("/")
            root = target_path[: min_idx + 1]
            depth = target_path[min_idx + 1 :].count("/") + 1
        else:
            root = ""
            depth = target_path[min_idx + 1 :].count("/") + 1
        if "**" in target_path:
            depth = None # Unlimited depth walk

        # Fix: HDFS throws IllegalArgumentException if root is "".
        if root == "":
            root = "/"

        pattern = re.compile(glob_translate(target_path))
        try:
            for dirpath, dirs, files in self._fs.walk(root, maxdepth=depth, detail=True):
                for name, info in files.items():
                    full_path: str = info["name"]
                    if target_path.startswith("/") and not full_path.startswith("/"):
                        full_path = "/" + full_path
                    if pattern.match(full_path) and _matches_filter(full_path, info):
                        yield (full_path, info)
        except (FileNotFoundError, OSError):
            return

    def _infer_unified_schema(self, files: t.Dict[str, t.Dict[str, t.Any]], parsed_request: FileRequest) -> pd.DataFrame:
        """
        Quickly scans the headers/metadata of all matched files in parallel and merges them to determine the global, unified schema.
        Returns the combined DataFrame, retaining the rows for downstream schema inference.
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
        return combined_df

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
            files = dict(self._lazy_glob(
                target_path, parsed_request.modified_since,
                require_file_exists=True, require_supported=True, strict_extension_check=True,
            ))
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
            files = dict(self._lazy_glob(
                target_path, parsed_request.modified_since,
                require_file_exists=True, require_supported=True, strict_extension_check=True,
            ))
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
            target_path = self._clean_path(parsed_request.file_pattern or parsed_request.file_path)
            files_to_remove = [path for path, _ in self._lazy_glob(target_path, parsed_request.modified_since, require_supported=False)]
            if files_to_remove:
                try:
                    self._fs.rm(files_to_remove, recursive=True)
                except:
                    for file_path in files_to_remove:
                        self._fs.rm(file_path, recursive=True)
        elif parsed_request.operation == FileOperationType.LIST:
            target_path = self._clean_path(parsed_request.file_pattern or parsed_request.file_path)
            self._last_df = pd.DataFrame({
                "file_path": pd.Series(dtype="str"),
                "file_name": pd.Series(dtype="str"),
                "file_size": pd.Series(dtype="int64"),
                "file_modification_time": pd.Series(dtype="datetime64[ns]") 
            })

            def list_generator():
                for path, info in self._lazy_glob(target_path, parsed_request.modified_since, require_supported=True):
                    yield (path, os.path.basename(path), info.get("Size", info.get("size")), self._normalize_mtime(info))
            self._row_iterator = list_generator()
        else:
            raise NotImplementedError(f"Operation {parsed_request.operation} is not implemented.")

    def _generate_read_chunks(self, files: t.Dict[str, t.Dict[str, t.Any]], parsed_request: FileRequest, unified_schema_df: t.Optional[pd.DataFrame] = None) -> t.Iterator[pd.DataFrame]:
        # Bounding the queue forces background threads to pause reading if the consumer gets too slow.
        chunk_queue = queue.Queue(maxsize=parsed_request.concurrency * 2)
        master_columns = list(unified_schema_df.columns) if unified_schema_df is not None else None
        stop_event = threading.Event()

        def worker(path, info):
            try:
                for chunk_df in self._read_file_chunks(path, info, parsed_request.options):
                    if stop_event.is_set():
                        return

                    if master_columns is not None:
                        # Missing columns become NaN. Unwanted extra columns are dropped.
                        processed_df = chunk_df.reindex(columns=master_columns)
                    else:
                        processed_df = chunk_df

                    while not stop_event.is_set():
                        try:
                            chunk_queue.put(("data", processed_df), timeout=1.0)
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
        file_path = self._clean_path(file_path)
        _, file_ext = os.path.splitext(file_path.lower())
        handler = FileHandlerFactory.get_handler(file_ext)
        if not handler:
            return 
        
        for df in handler.read(self._fs, file_path, file_options, self.METADATA_COLUMN_PREFIX):
            if df is not None and not df.empty:
                df[self.METADATA_COLUMN_PREFIX+"file_path"] = file_path
                df[self.METADATA_COLUMN_PREFIX+"file_name"] = os.path.basename(file_path)
                df[self.METADATA_COLUMN_PREFIX+"file_size"] = file_info.get("Size", file_info.get("size"))
                df[self.METADATA_COLUMN_PREFIX+"file_modification_time"] = self._normalize_mtime(file_info)
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
            df[self.METADATA_COLUMN_PREFIX+"file_modification_time"] = self._normalize_mtime(file_info)
        return df
    
    def _write_single_file(self, df: pd.DataFrame, file_path: str, file_options: t.Optional[FileOptions] = None) -> None:
        try:
            _, file_ext = os.path.splitext(file_path.lower())
            handler = FileHandlerFactory.get_handler(file_ext)
            if not handler:
                raise ValueError(f"Unsupported file format for writing: {file_path}")
            
            handler.write(self._fs, df, file_path, file_options, self.METADATA_COLUMN_PREFIX)
        except Exception as e:
            logger.error(f"Failed to write file to {file_path}.", exc_info=e)
            raise

    def _append_single_file(self, df: pd.DataFrame, file_path: str, file_options: t.Optional[FileOptions] = None) -> None:
        cleaned_path = self._clean_path(file_path)
        # Use _lazy_glob() instead of fsspec.exists()
        existing_files = list(self._lazy_glob(cleaned_path, require_supported=False))
        if not existing_files:
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
        if self._last_df is None:
            return pd.DataFrame()

        if self._row_iterator is None:
            return self._last_df.iloc[0:0] # Preserves schema
            
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