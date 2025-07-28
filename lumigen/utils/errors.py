class ConfigError(ValueError):
    pass

class DPError(Exception):
    pass

class EngineAdapterError(DPError):
    pass

class UnsupportedDataFrameError(DPError):
    pass

class UnsupportedOperationError(EngineAdapterError):
    pass

class UnsupportedCatalogOperationError(UnsupportedOperationError):
    pass

class TransformerError(DPError):
    pass

class MissingColumnError(TransformerError):
    pass

class InvalidOperationError(TransformerError):
    pass

class InvalidParameterError(TransformerError):
    pass