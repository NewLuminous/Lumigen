from __future__ import annotations
import typing as t
import os
import io
import pandas as pd

if t.TYPE_CHECKING:
    from fsspec import AbstractFileSystem
    from engines.fs.request_parser import FileOptions

from engines.fs.handlers.base import BaseFileHandler


class ExcelFileHandler(BaseFileHandler):
    """
    Handles reading, describing, writing, and appending Excel and ODF files.
    """
    def read(self, fs: AbstractFileSystem, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str) -> t.Iterator[pd.DataFrame]:
        _, file_ext = os.path.splitext(file_path.lower())
        
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

        with fs.open(file_path, "rb") as f:
            sheets_dict = pd.read_excel(f, sheet_name=None, engine=engine)
            for sheet_name, sheet_df in sheets_dict.items():
                sheet_df[metadata_prefix + "excel_sheet_name"] = sheet_name
                yield sheet_df

    def describe(self, fs: AbstractFileSystem, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str) -> t.Optional[pd.DataFrame]:
        _, file_ext = os.path.splitext(file_path.lower())
        
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

        with fs.open(file_path, "rb") as f:
            # Use nrows=1 to extract only one row without reading full data
            sheets_dict = pd.read_excel(f, sheet_name=None, engine=engine, nrows=1)
            for sheet_name, sheet_df in sheets_dict.items():
                sheet_df[metadata_prefix + "excel_sheet_name"] = sheet_name
            return pd.concat(sheets_dict.values(), ignore_index=True)

    def write(self, fs: AbstractFileSystem, df: pd.DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str) -> None:
        _, file_ext = os.path.splitext(file_path.lower())
        metadata_cols = [col for col in df.columns if str(col).startswith(metadata_prefix)]
        
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
        
        with fs.open(file_path, "wb") as f:
            sheet_name_col = metadata_prefix + "excel_sheet_name"
            with pd.ExcelWriter(f, engine=engine) as writer:
                if sheet_name_col in df.columns:
                    for sheet_name, group in df.groupby(sheet_name_col):
                        sheet_name_str = str(sheet_name) if pd.notna(sheet_name) else "Sheet1"
                        clean_group: pd.DataFrame = group.drop(columns=metadata_cols, errors="ignore")
                        clean_group.to_excel(writer, sheet_name=sheet_name_str, index=False)
                else:
                    clean_df: pd.DataFrame = df.drop(columns=metadata_cols, errors="ignore")
                    clean_df.to_excel(writer, sheet_name="Sheet1", index=False)

    def append(self, fs: AbstractFileSystem, df: pd.DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str) -> None:
        _, file_ext = os.path.splitext(file_path.lower())
        metadata_cols = [col for col in df.columns if str(col).startswith(metadata_prefix)]
        
        file_buffer = io.BytesIO()
        with fs.open(file_path, "rb") as f:
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
            
            sheet_name_col = metadata_prefix + "excel_sheet_name"
            sheets_to_append: t.Dict[str, pd.DataFrame] = {}
            if sheet_name_col in df.columns:
                for sheet_name, group in df.groupby(sheet_name_col):
                    sheet_name_str = str(sheet_name) if pd.notna(sheet_name) else "Sheet1"
                    sheets_to_append[sheet_name_str] = group.drop(columns=metadata_cols, errors="ignore")
            else:
                sheets_to_append["Sheet1"] = df.drop(columns=metadata_cols, errors="ignore")

            file_buffer.seek(0)
            excel_file = pd.ExcelFile(file_buffer, engine=engine)
            existing_sheet_names = excel_file.sheet_names
            
            # VALIDATE SCHEMA
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
                final_sheets: t.Dict[str, pd.DataFrame] = {}
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

            # Write the completed buffer back to the file system, replacing the old immutable object
            file_buffer.seek(0)
            with fs.open(file_path, "wb") as f:
                f.write(file_buffer.read())
        finally:
            file_buffer.close()