from engines.spark.file_handlers.base import SparkFileHandlerFactory, BaseSparkFileHandler
from engines.spark.file_handlers.excel import SparkExcelFileHandler
from engines.spark.file_handlers.orc import SparkORCFileHandler


SparkFileHandlerFactory.register(
    [".xls", ".xlsx", ".xlsm", ".xlsb", ".ods", ".xlt", ".xltx", ".xltm", ".ots"], 
    SparkExcelFileHandler
)
SparkFileHandlerFactory.register([".orc"], SparkORCFileHandler)

__all__ = ["SparkFileHandlerFactory", "BaseSparkFileHandler"]