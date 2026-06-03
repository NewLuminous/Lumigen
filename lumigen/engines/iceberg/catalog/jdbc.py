from __future__ import annotations
from pyiceberg.catalog import TYPE, URI
from pyiceberg.catalog.sql import SqlCatalog

from utils.connections import JdbcUriTranslator


class JDBCCatalog(SqlCatalog):
    """
    A PyIceberg Catalog that behaves like PySpark's JdbcCatalog.
    Intercepts Spark JDBC parameters and automatically provisions SQLAlchemy.
    """
    def __init__(self, name: str, **properties: str):
        # Translate URI and Authentication
        jdbc_uri = properties.get(URI, "")
        if jdbc_uri:
            user = properties.pop("jdbc.user", "")
            password = properties.pop("jdbc.password", "")
            properties[URI] = JdbcUriTranslator.translate_to_sqlalchemy(jdbc_uri, user, password)
            
        # Remove properties that cause SQLAlchemy/PyIceberg to throw Warnings or Errors
        properties.pop("catalog-impl", None)
        properties.pop(TYPE, None)
        
        super().__init__(name, **properties)