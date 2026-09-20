from engines.fs.handlers.base import FileHandlerFactory, BaseFileHandler
from engines.fs.handlers.csv import CSVFileHandler
from engines.fs.handlers.excel import ExcelFileHandler
from engines.fs.handlers.orc import ORCFileHandler
from engines.fs.handlers.text import TextFileHandler


FileHandlerFactory.register([".csv", ".tsv"], CSVFileHandler)
FileHandlerFactory.register(
    [".xls", ".xlsx", ".xlsm", ".xlsb", ".ods", ".xlt", ".xltx", ".xltm", ".ots"], 
    ExcelFileHandler
)
FileHandlerFactory.register([".orc"], ORCFileHandler)
FileHandlerFactory.register([".txt", ".log", ".dat"], TextFileHandler)

__all__ = ["FileHandlerFactory", "BaseFileHandler"]