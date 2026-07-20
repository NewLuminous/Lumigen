from __future__ import annotations
import typing as t
import logging
import os
import re
from s3fs import S3FileSystem
import pandas as pd
from pandas import DataFrame
from concurrent.futures import ThreadPoolExecutor

from engines.fs.request_parser import FileRequestParser, FileOperationType, FileOptions, RequestParseError
from utils.dataset import columns_to_types_from_df

logger = logging.getLogger(__name__)


class S3Cursor:
    METADATA_COLUMN_PREFIX = "_metadata_"

    def __init__(self, fs: S3FileSystem):
        self._fs = fs
        self._last_df: t.Optional[DataFrame] = None
        self._row_iterator: t.Optional[t.Iterator[t.Tuple]] = None

    def close(self) -> None:
        pass

    def execute(self, request: str, payload: t.Optional[t.Any] = None, **kwargs) -> None:
        self._last_df = None
        self._row_iterator = None

        try:
            parsed_request = FileRequestParser.parse(request)
        except RequestParseError as e:
            logger.error(f"Failed to execute request due to parsing error.", exc_info=e)
            raise

        if parsed_request.operation == FileOperationType.READ:
            files: t.Dict[str, t.Dict[str, t.Any]] = self._fs.glob(parsed_request.file_pattern or parsed_request.file_path, detail=True)
            if parsed_request.modified_since is not None:
                files = {
                    path: info for path, info in files.items()
                    if info.get("LastModified", info.get("mtime")) is not None 
                    and info.get("LastModified", info.get("mtime")) > parsed_request.modified_since
                }
            if not files:
                return

            df_list = []
            with ThreadPoolExecutor(max_workers=parsed_request.concurrency) as executor:
                futures = [
                    executor.submit(self._read_single_file, path, info, parsed_request.options) 
                    for path, info in files.items()
                ]
                for future in futures:
                    try:
                        res = future.result()
                        if res is not None and not res.empty:
                            df_list.append(res)
                    except Exception as e:
                        logger.error(f"Error processing file in thread: {e}")

            if df_list:
                self._last_df = pd.concat(df_list, ignore_index=True)
                self._row_iterator = self._last_df.itertuples(index=False, name=None)
        elif parsed_request.operation == FileOperationType.DESCRIBE:
            files: t.Dict[str, t.Dict[str, t.Any]] = self._fs.glob(parsed_request.file_pattern or parsed_request.file_path, detail=True)
            if parsed_request.modified_since is not None:
                files = {
                    path: info for path, info in files.items()
                    if info.get("LastModified", info.get("mtime")) is not None 
                    and info.get("LastModified", info.get("mtime")) > parsed_request.modified_since
                }
            if not files:
                self._last_df = pd.DataFrame(columns=["field", "type"])
                return

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
                        logger.error(f"Error processing file in thread: {e}")

            if df_list:
                combined_schema_df: DataFrame = pd.concat(df_list, ignore_index=True)
                self._last_df = pd.DataFrame(
                    {"field": name, "type": dtype.sql()}
                    for name, dtype in columns_to_types_from_df(combined_schema_df).items()
                )
                self._row_iterator = self._last_df.itertuples(index=False, name=None)
            else:
                self._last_df = pd.DataFrame(columns=["field", "type"])
        elif parsed_request.operation == FileOperationType.WRITE:
            if isinstance(payload, DataFrame):
                df = payload
            else:
                raise ValueError(f"Unsupported payload type: {type(payload)}.")
            
            self._write_single_file(df, parsed_request.file_path, parsed_request.options)
        elif parsed_request.operation == FileOperationType.APPEND:
            if isinstance(payload, DataFrame):
                df = payload
            else:
                raise ValueError(f"Unsupported payload type: {type(payload)}.")
            
            self._append_single_file(df, parsed_request.file_path, parsed_request.options)
        elif parsed_request.operation == FileOperationType.REMOVE:
            if parsed_request.file_path:
                cleaned_path = re.sub(r'^s3[a-z]?://', '', parsed_request.file_path)
                if self._fs.exists(cleaned_path):
                    self._fs.rm(cleaned_path, recursive=True)
            if parsed_request.file_pattern:
                cleaned_path = re.sub(r'^s3[a-z]?://', '', parsed_request.file_pattern)
                files_to_remove = self._fs.glob(cleaned_path)
                if files_to_remove:
                    self._fs.rm(files_to_remove, recursive=True)
        elif parsed_request.operation == FileOperationType.LIST:
            files: t.Dict[str, t.Dict[str, t.Any]] = self._fs.glob(parsed_request.file_pattern or parsed_request.file_path, detail=True)
            # Only list supported file extensions
            files = {
                path: info for path, info in files.items()
                if os.path.splitext(path.lower())[-1] in (".xls", ".xlsx", ".xlsm", ".xlsb", ".ods", ".xlt", ".xltx", ".xltm", ".ots")
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
    
    def _read_single_file(self, file_path: str, file_info: t.Dict[str, t.Any] = {}, file_options: t.Optional[FileOptions] = None) -> t.Optional[DataFrame]:
        _, file_ext = os.path.splitext(file_path.lower())
        # Excel
        if file_ext in (".xls", ".xlsx", ".xlsm", ".xlsb", ".ods", ".xlt", ".xltx", ".xltm", ".ots"):
            with self._fs.open(file_path, "rb") as f:
                has_calamine = False
                try:
                    import python_calamine
                    has_calamine = pd.__version__ >= "2.1.0"
                except ImportError:
                    pass

                engine_map = {
                    ".xls": "xlrd",
                    ".xlt": "xlrd",
                    ".xlsx": "calamine" if has_calamine else "openpyxl",
                    ".xltx": "calamine" if has_calamine else "openpyxl",
                    ".xlsm": "calamine" if has_calamine else "openpyxl",
                    ".xltm": "calamine" if has_calamine else "openpyxl",
                    ".xlsb": "calamine" if has_calamine else "pyxlsb",
                    ".ods": "calamine" if has_calamine else "odf",
                    ".ots": "calamine" if has_calamine else "odf",
                }
                engine = engine_map.get(file_ext)
                
                sheets_dict = pd.read_excel(f, sheet_name=None, engine=engine)
                for sheet_name, sheet_df in sheets_dict.items():
                    sheet_df[self.METADATA_COLUMN_PREFIX+"excel_sheet_name"] = sheet_name
                df = pd.concat(sheets_dict.values(), ignore_index=True)
        else:
            df = None

        if df is not None:
            df[self.METADATA_COLUMN_PREFIX+"file_path"] = file_path
            df[self.METADATA_COLUMN_PREFIX+"file_name"] = os.path.basename(file_path)
            df[self.METADATA_COLUMN_PREFIX+"file_size"] = file_info.get("Size", file_info.get("size"))
            df[self.METADATA_COLUMN_PREFIX+"file_modification_time"] = file_info.get("LastModified", file_info.get("mtime"))
        return df
    
    def _describe_single_file(self, file_path: str, file_info: t.Dict[str, t.Any] = {}, file_options: t.Optional[FileOptions] = None) -> t.Optional[DataFrame]:
        _, file_ext = os.path.splitext(file_path.lower())
        # Excel
        if file_ext in (".xls", ".xlsx", ".xlsm", ".xlsb", ".ods", ".xlt", ".xltx", ".xltm", ".ots"):
            with self._fs.open(file_path, "rb") as f:
                has_calamine = False
                try:
                    import python_calamine
                    has_calamine = pd.__version__ >= "2.1.0"
                except ImportError:
                    pass

                engine_map = {
                    ".xls": "xlrd",
                    ".xlt": "xlrd",
                    ".xlsx": "calamine" if has_calamine else "openpyxl",
                    ".xltx": "calamine" if has_calamine else "openpyxl",
                    ".xlsm": "calamine" if has_calamine else "openpyxl",
                    ".xltm": "calamine" if has_calamine else "openpyxl",
                    ".xlsb": "calamine" if has_calamine else "pyxlsb",
                    ".ods": "calamine" if has_calamine else "odf",
                    ".ots": "calamine" if has_calamine else "odf",
                }
                engine = engine_map.get(file_ext)
                
                # Use nrows=1 to extract only one row without reading full data
                sheets_dict = pd.read_excel(f, sheet_name=None, engine=engine, nrows=1)
                for sheet_name, sheet_df in sheets_dict.items():
                    sheet_df[self.METADATA_COLUMN_PREFIX+"excel_sheet_name"] = sheet_name
                df = pd.concat(sheets_dict.values(), ignore_index=True)
        else:
            df = None

        if df is not None:
            df[self.METADATA_COLUMN_PREFIX+"file_path"] = file_path
            df[self.METADATA_COLUMN_PREFIX+"file_name"] = os.path.basename(file_path)
            df[self.METADATA_COLUMN_PREFIX+"file_size"] = file_info.get("Size", file_info.get("size"))
            df[self.METADATA_COLUMN_PREFIX+"file_modification_time"] = file_info.get("LastModified", file_info.get("mtime"))
        return df
    
    def _write_single_file(self, df: DataFrame, file_path: str, file_options: t.Optional[FileOptions] = None) -> t.Optional[DataFrame]:
        _, file_ext = os.path.splitext(file_path.lower())
        metadata_cols = [col for col in df.columns if str(col).startswith(self.METADATA_COLUMN_PREFIX)]
        try:
            # Excel
            if file_ext in (".xls", ".xlsx", ".xlsm", ".xlsb", ".ods", ".xlt", ".xltx", ".xltm", ".ots"):
                with self._fs.open(file_path, "wb") as f:
                    if file_ext in (".xlsx", ".xlsm", ".xltx", ".xltm"):
                        try:
                            import xlsxwriter
                            engine = "xlsxwriter"
                        except ImportError:
                            engine = "openpyxl"
                    elif file_ext in (".ods", ".ots"):
                        engine = "odf"
                    elif file_ext in (".xls", ".xlt", ".xlsb"):
                        raise ValueError(f"Writing {file_ext} files is not supported by Pandas. Please use .xlsx instead.")
                    else:
                        engine = "auto"
                
                    sheet_name_col = self.METADATA_COLUMN_PREFIX + "excel_sheet_name"
                    with pd.ExcelWriter(f, engine=engine) as writer:
                        if sheet_name_col in df.columns:
                            for sheet_name, group in df.groupby(sheet_name_col):
                                sheet_name_str = str(sheet_name) if pd.notna(sheet_name) else "Sheet1"
                                clean_group: DataFrame = group.drop(columns=metadata_cols, errors="ignore")
                                clean_group.to_excel(writer, sheet_name=sheet_name_str, index=False)
                        else:
                            clean_df: DataFrame = df.drop(columns=metadata_cols, errors="ignore")
                            clean_df.to_excel(writer, sheet_name="Sheet1", index=False)
            else:
                raise ValueError(f"Unsupported file format for writing: {file_path}")
        except Exception as e:
            logger.error(f"Failed to write file to {file_path}.", exc_info=e)
            raise

    def _append_single_file(self, df: DataFrame, file_path: str, file_options: t.Optional[FileOptions] = None) -> t.Optional[DataFrame]:
        cleaned_path = re.sub(r'^s3[a-z]?://', '', file_path)
        if not self._fs.exists(cleaned_path):
            return self._write_single_file(df, file_path, file_options)

        _, file_ext = os.path.splitext(file_path.lower())
        metadata_cols = [col for col in df.columns if str(col).startswith(self.METADATA_COLUMN_PREFIX)]
        try:
            # Excel
            if file_ext in (".xls", ".xlsx", ".xlsm", ".xlsb", ".ods", ".xlt", ".xltx", ".xltm", ".ots"):
                import io
                file_buffer = io.BytesIO()
                with self._fs.open(file_path, "rb") as f:
                    file_buffer.write(f.read())

                try:
                    if file_ext in (".xlsx", ".xlsm", ".xltx", ".xltm"):
                        engine = "openpyxl"
                        can_overlay = True
                    elif file_ext in (".ods", ".ots"):
                        engine = "odf"
                        can_overlay = False # ODF engine doesn't natively support overlay/append mode well
                    else:
                        raise ValueError(f"Append is not supported for {file_ext} files. Please use .xlsx.")
                    
                    sheet_name_col = self.METADATA_COLUMN_PREFIX + "excel_sheet_name"
                    sheets_to_append: t.Dict[str, DataFrame] = {}
                    if sheet_name_col in df.columns:
                        for sheet_name, group in df.groupby(sheet_name_col):
                            sheet_name_str = str(sheet_name) if pd.notna(sheet_name) else "Sheet1"
                            sheets_to_append[sheet_name_str] = group.drop(columns=metadata_cols, errors="ignore")
                    else:
                        sheets_to_append["Sheet1"] = df.drop(columns=metadata_cols, errors="ignore")

                    file_buffer.seek(0)
                    excel_file = pd.ExcelFile(file_buffer, engine=engine)
                    existing_sheet_names = excel_file.sheet_names
                    # VALIDATE SCHEMA: Check headers before any writing
                    for sheet_name, new_df in sheets_to_append.items():
                        if sheet_name in existing_sheet_names:
                            existing_schema_df = pd.read_excel(excel_file, sheet_name=sheet_name, nrows=0)
                            existing_cols = list(existing_schema_df.columns)
                            new_cols = list(new_df.columns)
                            if existing_cols != new_cols:
                                raise ValueError(
                                    f"Schema mismatch for sheet '{sheet_name}'.\n"
                                    f"Expected: {existing_cols}\n"
                                    f"Got:      {new_cols}"
                                )
                            
                    if can_overlay:
                        import openpyxl
                        
                        # Find the maximum row for each target sheet so we know where to append
                        start_rows = {}
                        file_buffer.seek(0)
                        wb = openpyxl.load_workbook(file_buffer, read_only=True)
                        for sheet_name in sheets_to_append.keys():
                            if sheet_name in existing_sheet_names:
                                # max_row gives the 1-based index of the last row.
                                # Setting startrow=max_row in Pandas 0-indexes to the exact next empty row!
                                start_rows[sheet_name] = wb[sheet_name].max_row
                            else:
                                start_rows[sheet_name] = 0
                        wb.close()

                        file_buffer.seek(0)
                        # mode='a' with if_sheet_exists='overlay' modifies the workbook in place 
                        with pd.ExcelWriter(file_buffer, engine=engine, mode="a", if_sheet_exists="overlay") as writer:
                            for sheet_name, new_df in sheets_to_append.items():
                                start_row = start_rows[sheet_name]
                                if start_row > 0:
                                    # Append to existing sheet: NO header, start exactly below the last row
                                    new_df.to_excel(writer, sheet_name=sheet_name, startrow=start_row, index=False, header=False)
                                else:
                                    # Brand new sheet: write normally with header
                                    new_df.to_excel(writer, sheet_name=sheet_name, index=False, header=True)
                    else:
                        final_sheets: t.Dict[str, DataFrame] = {}
                        for sheet_name in existing_sheet_names:
                            if sheet_name in sheets_to_append:
                                existing_df = pd.read_excel(excel_file, sheet_name=sheet_name)
                                final_sheets[sheet_name] = pd.concat([existing_df, sheets_to_append.pop(sheet_name)], ignore_index=True)
                            else:
                                final_sheets[sheet_name] = pd.read_excel(excel_file, sheet_name=sheet_name)
                        
                        # Include remaining brand new sheets
                        for sheet_name, new_df in sheets_to_append.items():
                            final_sheets[sheet_name] = new_df

                        file_buffer = io.BytesIO() # Reset buffer
                        with pd.ExcelWriter(file_buffer, engine=engine) as writer:
                            for sheet_name, sheet_df in final_sheets.items():
                                sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)

                    # Write the completed buffer back to S3, replacing the old immutable object
                    file_buffer.seek(0)
                    with self._fs.open(file_path, "wb") as f:
                        f.write(file_buffer.read())
                finally:
                    file_buffer.close()
            else:
                raise ValueError(f"Unsupported file format for appending: {file_path}")
        except Exception as e:
            logger.error(f"Failed to append data to file {file_path}.", exc_info=e)
            raise

    def fetchdf(self) -> DataFrame:
        return self._last_df if self._last_df is not None else pd.DataFrame()
        
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
        return len(self._last_df) if self._last_df is not None else 0

class S3Connection:
    def __init__(self, endpoint_url: t.Optional[str] = None,
                 s3_access_key_id: t.Optional[str] = None, s3_secret_access_key: t.Optional[str] = None,
                 client_config: t.Optional[t.Dict[str, t.Any]] = None):
        self.endpoint_url = endpoint_url
        self.s3_access_key_id = s3_access_key_id
        self.s3_secret_access_key= s3_secret_access_key
        self.client_config = client_config or {}

        self._fs: t.Optional[S3FileSystem] = None

    @property
    def fs(self) -> S3FileSystem:
        if self._fs is None:
            key = self.s3_access_key_id or self.client_config.get("aws_access_key_id")
            secret = self.s3_secret_access_key or self.client_config.get("aws_secret_access_key")
            anon = not key or not secret
            self._fs = S3FileSystem(
                endpoint_url=self.endpoint_url,
                key=self.s3_access_key_id,
                secret=self.s3_secret_access_key,
                anon=anon,
                client_kwargs=self.client_config,
            )

        return self._fs
    
    def get_fs(self) -> S3FileSystem:
        return self.fs
    
    def ping(self) -> None:
        self.fs.call_s3('list_buckets')

    def exists(self, file_path: str) -> bool:
        stripped_path = re.sub(r'^s3[a-z]?://', '', file_path)
        return self.fs.exists(stripped_path)
    
    def cursor(self) -> S3Cursor:
        return S3Cursor(fs=self.fs)
    
    def commit(self) -> None:
        """
        Satisfies PEP-249 DB-API requirements for Pandas.
        Note: Actual WAP transactions in this engine are managed and committed locally by the IcebergCursor.
        """
        pass

    def rollback(self) -> None:
        """
        Satisfies PEP-249 DB-API requirements for Pandas error-handling.
        Note: Actual WAP rollbacks are handled locally by the IcebergCursor.
        """
        pass

    def close(self) -> None:
        """Closes the connection object. Cursors must be closed separately."""
        pass

def connect(endpoint_url: t.Optional[str] = None,
            s3_access_key_id: t.Optional[str] = None, s3_secret_access_key: t.Optional[str] = None,
            client_config: t.Optional[t.Dict[str, t.Any]] = None,
            ) -> S3Connection:
    return S3Connection(endpoint_url, s3_access_key_id, s3_secret_access_key, client_config)