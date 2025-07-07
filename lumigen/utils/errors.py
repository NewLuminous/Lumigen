class ConfigError(ValueError):
    pass

class DPError(Exception):
    pass

class EngineAdapterError(DPError):
    pass

class UnsupportedDataFrameError(DPError):
    pass

class UnsupportedCatalogOperationError(EngineAdapterError):
    pass

class TransformerError(DPError):
    pass

class MissingColumnError(TransformerError):
    pass

class InvalidOperationError(TransformerError):
    pass

class InvalidParameterError(TransformerError):
    pass