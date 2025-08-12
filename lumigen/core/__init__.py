from core.adapters import register_adapter_resolver
from core.engine_adapter.spark.resolver import SparkAdapterResolver


# --- PLUG-IN REGISTRATION ---
register_adapter_resolver("spark", SparkAdapterResolver)
# register_adapter_resolver("flink", FlinkAdapterResolver)