from __future__ import annotations
import typing as t
import re
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode, quote, unquote, urlparse


class JdbcUriTranslator:
    """
    Utility class dedicated to translating connection URIs between Java JDBC and Python SQLAlchemy.
    """
    
    DIALECT_MAP = {
        "mysql": "mysql+pymysql",
        "postgresql": "postgresql+psycopg2",
        "postgres": "postgresql+psycopg2",
        "db2": "db2+ibm_db",
        "kingbase": "kingbase+ksycopg2",
        "kingbase8": "kingbase+ksycopg2",
        "oceanbase": "oceanbase+pymysql",
        "redshift": "redshift+psycopg2",
        "mssql": "mssql+pyodbc",
        "sqlserver": "mssql+pyodbc",
        "oracle": "oracle+oracledb",
        "snowflake": "snowflake",
        "hivejdbc": "hive",
        "hive2": "hive",
        "phoenix": "phoenix",
        "vertica": "vertica+vertica_python",
    }

    REVERSE_DIALECT_MAP = {
        "mssql": "sqlserver",
        "postgres": "postgresql",
    }

    @classmethod
    def _normalize_uri_for_parsing(cls, uri: str) -> t.Tuple[str, bool]:
        """Strips JDBC prefixes and normalizes edge cases so urlsplit works reliably."""
        is_jdbc = uri.startswith("jdbc:")
        uri_core = uri[len("jdbc:"):] if is_jdbc else uri

        # Oracle Thin: host:port:SID or //host:port/SERVICE, with or without user/password@.
        # SQLAlchemy reads the URL database as the SID, a service goes in ?service_name=.
        match_oracle = re.match(
            r"^oracle:thin:(?:([^/@]+)/([^@]+))?@(?://)?([^/:]+):(\d+)([:/])([^?]+)(?:\?(.*))?$",
            uri_core,
        )
        if match_oracle:
            u, p, host, port, separator, db, query = match_oracle.groups()
            auth = f"{quote(u)}:{quote(p)}@" if u else ""
            params = [f"service_name={quote(db)}"] if separator == "/" else []
            params += [query] if query else []
            target = f"/{db}" if separator == ":" else "/"
            uri_core = f"oracle://{auth}{host}:{port}{target}" + (f"?{'&'.join(params)}" if params else "")

        # SQL Server format: sqlserver://host;db=x;user=y
        if uri_core.startswith("sqlserver://") or uri_core.startswith("mssql+pyodbc://"):
            parts = uri_core.split(";", 1)
            if len(parts) == 2:
                uri_core = f"{parts[0]}?{parts[1].replace(';', '&')}"

        return uri_core, is_jdbc

    @classmethod
    def translate_to_sqlalchemy(cls, uri: str, user: str = "", password: str = "") -> str:
        """Translates a Java JDBC URI into a Python SQLAlchemy URI."""
        uri_core, is_jdbc = cls._normalize_uri_for_parsing(uri)
        parsed = urlsplit(uri_core)

        # Extract and resolve credentials
        q_params = dict(parse_qsl(parsed.query))
        q_user = q_params.pop("user", None) or q_params.pop("username", None)
        q_pass = q_params.pop("password", None)
        netloc_user = unquote(parsed.username) if parsed.username else None
        netloc_pass = unquote(parsed.password) if parsed.password else None

        # Resolve final credentials (Priority: 1. Kwargs, 2. Netloc, 3. Query string)
        final_user = user or netloc_user or q_user or ""
        final_pass = password or netloc_pass or q_pass or ""

        # Percent-encode for safe embedding (e.g. P@$$w0rd -> P%40%24%24w0rd)
        enc_user = quote(final_user, safe="") if final_user else ""
        enc_pass = quote(final_pass, safe="") if final_pass else ""
        auth_str = f"{enc_user}:{enc_pass}@" if enc_user and enc_pass else (f"{enc_user}@" if enc_user else "")

        # Reconstruct Netloc
        hostname = parsed.hostname or ""
        port_str = f":{parsed.port}" if parsed.port else ""
        new_netloc = f"{auth_str}{hostname}{port_str}"

        scheme = parsed.scheme.lower()

        # Specialized SQLite translation rules
        if scheme == "sqlite":
            if is_jdbc:
                remainder = uri[len("jdbc:sqlite:"):]
                if remainder == ":memory:":
                    return "sqlite:///:memory:"
                elif remainder.startswith("///"):
                    # Absolute path with 3 slashes -> SQLAlchemy needs 4 slashes
                    return f"sqlite:////{remainder[3:]}"
                elif remainder.startswith("//"):
                    return f"sqlite:///{remainder[2:]}"
                elif remainder.startswith("/"):
                    # Absolute path with 1 slash -> SQLAlchemy needs 4 slashes
                    return f"sqlite:////{remainder[1:]}"
                else:
                    # Relative path (e.g., test.db) -> SQLAlchemy needs 3 slashes
                    return f"sqlite:///{remainder}"
            else:
                new_scheme = scheme
        else:
            new_scheme = cls.DIALECT_MAP.get(scheme, scheme) if is_jdbc else scheme
        
        # Reconstruct and return the final URI
        new_query = urlencode(q_params)
        return urlunsplit((new_scheme, new_netloc, parsed.path, new_query, parsed.fragment))
    
    @classmethod
    def translate_to_jdbc(cls, uri: str, user: str = "", password: str = "") -> str:
        """Translates a Python SQLAlchemy URI back into a Java JDBC URI."""
        if "://" not in uri and not uri.startswith("jdbc:sqlite:") and not uri.startswith("sqlite:"):
            # Obscure edge cases that aren't valid URIs at all
            if not uri.startswith("jdbc:oracle:thin:"):
                return uri

        uri_core, is_jdbc = cls._normalize_uri_for_parsing(uri)
        parsed = urlsplit(uri_core)

        # Extract and resolve credentials
        q_params = dict(parse_qsl(parsed.query))
        q_user = q_params.pop("user", None) or q_params.pop("username", None)
        q_pass = q_params.pop("password", None)
        
        netloc_user = unquote(parsed.username) if parsed.username else None
        netloc_pass = unquote(parsed.password) if parsed.password else None
        
        # Priority: 1. Kwargs, 2. Netloc, 3. Query string
        final_user = user or netloc_user or q_user or ""
        final_pass = password or netloc_pass or q_pass or ""
        
        # Extract base dialect (e.g., 'mysql+pymysql' -> 'mysql')
        scheme_parts = parsed.scheme.split("+")
        base_dialect = scheme_parts[0].lower()
        
        # Handle SQLite
        if base_dialect == "sqlite":
            if is_jdbc:
                # Credentials usually don't matter for SQLite JDBC, return as-is
                return uri
            else:
                path = parsed.path
                if path == "/:memory:":
                    return "jdbc:sqlite::memory:"
                elif path.startswith("//"): 
                    # Absolute path: sqlite:////tmp/db.sqlite -> jdbc:sqlite:/tmp/db.sqlite
                    return f"jdbc:sqlite:{path[1:]}" 
                elif path.startswith("/"): 
                    # Relative path: sqlite:///tmp/db.sqlite -> jdbc:sqlite:tmp/db.sqlite
                    return f"jdbc:sqlite:{path[1:]}"
                else:
                    return f"jdbc:sqlite:{path}"

        # Reconstruct network location without credentials
        hostname = parsed.hostname or ""
        port_str = f":{parsed.port}" if parsed.port else ""
        netloc = f"{hostname}{port_str}"
        path = parsed.path

        # Handle Driver-Specific Edge Cases
        if base_dialect == "mssql" or (is_jdbc and base_dialect == "sqlserver"):
            # SQL Server uses semicolons for attributes instead of standard HTTP query params
            db_name = path.lstrip("/")
            parts = [f"jdbc:sqlserver://{netloc}"]
            if db_name:
                parts.append(f"databaseName={db_name}")
            if final_user:
                parts.append(f"user={final_user}")
            if final_pass:
                parts.append(f"password={final_pass}")
            for k, v in q_params.items():
                parts.append(f"{k}={v}")
            return ";".join(parts)
        elif base_dialect == "oracle":
            # Oracle Thin format: the URL database is the SID (host:port:SID), ?service_name= a service (//host:port/SERVICE)
            service_name = q_params.pop("service_name", None)
            sid = path.lstrip("/")
            target = f"//{netloc}/{service_name}" if service_name else (f"{netloc}:{sid}" if sid else netloc)
            if final_user and final_pass:
                base_url = f"jdbc:oracle:thin:{quote(final_user, safe='')}/{quote(final_pass, safe='')}@{target}"
            elif final_user:
                base_url = f"jdbc:oracle:thin:{quote(final_user, safe='')}@{target}"
            else:
                base_url = f"jdbc:oracle:thin:@{target}"
            
            # Oracle rarely uses query strings, but append if present
            new_query = urlencode(q_params)
            return f"{base_url}?{new_query}" if new_query else base_url
        else:
            # Handle Standard JDBC (MySQL, PostgreSQL, Redshift, Snowflake, etc.)
            jdbc_scheme = base_dialect if is_jdbc else cls.REVERSE_DIALECT_MAP.get(base_dialect, base_dialect)
            
            # JDBC universally accepts credentials as query parameters
            if final_user:
                q_params["user"] = final_user
            if final_pass:
                q_params["password"] = final_pass

            new_query = urlencode(q_params)
            new_uri = f"jdbc:{jdbc_scheme}://{netloc}{path}"
            
            if new_query:
                new_uri += f"?{new_query}"
                
            return new_uri
    

class HadoopS3PropertiesSyncer:
    """
    Utility class dedicated to bidirectional in-place synchronization of 
    storage configurations between Hadoop (fs.s3a.*) and Python S3 (s3.*).
    """

    # Ref: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/core-default.xml
    # Ref: https://github.com/apache/iceberg/blob/main/aws/src/main/java/org/apache/iceberg/aws/s3/S3FileIOProperties.java
    HADOOP_TO_S3_MAP = {
        "fs.s3a.endpoint": "s3.endpoint",
        "fs.s3a.access.key": "s3.access-key-id",
        "fs.s3a.secret.key": "s3.secret-access-key",
        "fs.s3a.session.token": "s3.session-token",
        "fs.s3a.endpoint.region": "s3.region",
        "fs.s3a.path.style.access": "s3.path-style-access",
        "fs.s3a.etag.checksum.enabled": "s3.checksum-enabled",
        "fs.s3a.server-side-encryption-algorithm": "s3.sse.type",
        "fs.s3a.server-side-encryption.key": "s3.sse.key",
        "fs.s3a.server-side-encryption.custom.key": "s3.sse.key",
        "fs.s3a.connection.establish.timeout": "s3.connect-timeout",
        "fs.s3a.connection.timeout": "s3.request-timeout",
        "fs.s3a.attempts.maximum": "s3.connect.max-retries",
        "fs.s3a.multipart.size": "s3.multipart.part-size-bytes",
        "fs.s3a.multipart.threshold": "s3.multipart.threshold",
        "fs.s3a.threads.max": "s3.multipart.num-threads",
        "fs.s3a.acl.default": "s3.acl",
        "fs.s3a.buffer.dir": "s3.staging-dir",
    }

    S3_TO_HADOOP_MAP = {
        "s3.endpoint": "fs.s3a.endpoint",
        "s3.access-key-id": "fs.s3a.access.key",
        "s3.secret-access-key": "fs.s3a.secret.key",
        "s3.session-token": "fs.s3a.session.token",
        "s3.region": "fs.s3a.endpoint.region",
        "s3.path-style-access": "fs.s3a.path.style.access",
        "s3.checksum-enabled": "fs.s3a.etag.checksum.enabled",
        "s3.sse.type": "fs.s3a.server-side-encryption-algorithm",
        "s3.connect-timeout": "fs.s3a.connection.establish.timeout",
        "s3.request-timeout": "fs.s3a.connection.timeout",
        "s3.connect.max-retries": "fs.s3a.attempts.maximum",
        "s3.multipart.part-size-bytes": "fs.s3a.multipart.size",
        "s3.multipart.threshold": "fs.s3a.multipart.threshold",
        "s3.multipart.num-threads": "fs.s3a.threads.max",
        "s3.delete.num-threads": "fs.s3a.threads.max",
        "s3.acl": "fs.s3a.acl.default",
        "s3.staging-dir": "fs.s3a.buffer.dir",
    }

    @staticmethod
    def _parse_hadoop_size(size_str: str) -> str:
        if not size_str:
            return size_str
        match = re.match(r'^(\d+)([kKmMgGtT])?$', str(size_str).strip())
        if not match:
            return str(size_str)
        
        val, suffix = match.groups()
        val = int(val)
        if suffix:
            suffix = suffix.upper()
            if suffix == 'K': val *= 1024
            elif suffix == 'M': val *= 1024**2
            elif suffix == 'G': val *= 1024**3
            elif suffix == 'T': val *= 1024**4
        return str(val)

    @classmethod
    def sync_inplace(cls, hadoop_properties: t.Dict[str, str], s3_properties: t.Dict[str, str]) -> None:
        """
        Cross-pollinates missing properties between a Hadoop configuration dictionary 
        and a S3 configuration dictionary in-place.
        """
        reverse_hadoop_map = {v: k for k, v in cls.HADOOP_TO_S3_MAP.items()}

        # 1. Sync from Hadoop -> S3
        for h_k, h_v in list(hadoop_properties.items()):
            if h_k in cls.HADOOP_TO_S3_MAP:
                s3_k = cls.HADOOP_TO_S3_MAP[h_k]
                if s3_k not in s3_properties:
                    if h_k in ("fs.s3a.multipart.size", "fs.s3a.multipart.threshold"):
                        s3_properties[s3_k] = cls._parse_hadoop_size(h_v)
                    else:
                        s3_properties[s3_k] = h_v

        # Hadoop's thread pool maps to two separate thread pools in Iceberg
        if "fs.s3a.threads.max" in hadoop_properties:
            s3_properties.setdefault("s3.delete.num-threads", hadoop_properties["fs.s3a.threads.max"])

        # 2. Sync from S3 -> Hadoop
        for s3_k, s3_v in list(s3_properties.items()):
            if s3_k in cls.S3_TO_HADOOP_MAP:
                h_k = cls.S3_TO_HADOOP_MAP[s3_k]
                if h_k not in hadoop_properties:
                    hadoop_properties[h_k] = str(s3_v)

        # Iceberg's unified SSE key maps to two different keys in Hadoop depending on the SSE algorithm
        if "s3.sse.key" in s3_properties:
            sse_type = str(s3_properties.get("s3.sse.type", "")).upper()
            if sse_type == "SSE-C":
                hadoop_properties.setdefault("fs.s3a.server-side-encryption.custom.key", s3_properties["s3.sse.key"])
            else:
                hadoop_properties.setdefault("fs.s3a.server-side-encryption.key", s3_properties["s3.sse.key"])

        # 3. Handle special multi-variable mapping: Proxy Configuration
        # Hadoop -> S3 Proxy
        if "s3.proxy-uri" not in s3_properties and "fs.s3a.proxy.host" in hadoop_properties:
            proxy_host = hadoop_properties["fs.s3a.proxy.host"]
            proxy_port = hadoop_properties.get("fs.s3a.proxy.port", "")
            proxy_user = hadoop_properties.get("fs.s3a.proxy.username", "")
            proxy_pass = hadoop_properties.get("fs.s3a.proxy.password", "")
            enc_user = quote(proxy_user, safe="") if proxy_user else ""
            enc_pass = quote(proxy_pass, safe="") if proxy_pass else ""
            auth = f"{enc_user}:{enc_pass}@" if enc_user and enc_pass else (f"{enc_user}@" if enc_user else "")
            port_str = f":{proxy_port}" if proxy_port else ""
            s3_properties["s3.proxy-uri"] = f"http://{auth}{proxy_host}{port_str}"

        # S3 -> Hadoop Proxy
        if "s3.proxy-uri" in s3_properties and "fs.s3a.proxy.host" not in hadoop_properties:
            parsed = urlparse(s3_properties["s3.proxy-uri"])
            if parsed.hostname:
                hadoop_properties["fs.s3a.proxy.host"] = parsed.hostname
            if parsed.port:
                hadoop_properties["fs.s3a.proxy.port"] = str(parsed.port)
            if parsed.username:
                hadoop_properties["fs.s3a.proxy.username"] = unquote(parsed.username)
            if parsed.password:
                hadoop_properties["fs.s3a.proxy.password"] = unquote(parsed.password)

        # 4. Handle special mapping: Anonymous Credentials
        anon_providers = [
            "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
            "software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider"
        ]
        
        # Hadoop -> S3 Anonymous
        if hadoop_properties.get("fs.s3a.aws.credentials.provider") in anon_providers:
            s3_properties.setdefault("s3.anonymous", "true")
            s3_properties.setdefault("s3.access-key-id", "")
            s3_properties.setdefault("s3.secret-access-key", "")

        # S3 -> Hadoop Anonymous
        is_s3_anon = str(s3_properties.get("s3.anonymous", "")).lower() == "true" or (
            "s3.access-key-id" in s3_properties and not s3_properties["s3.access-key-id"]
        )
        if is_s3_anon and "fs.s3a.aws.credentials.provider" not in hadoop_properties:
            hadoop_properties["fs.s3a.aws.credentials.provider"] = "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"

        # 5. Normalize Booleans in both property dicts to ensure lowercase consistency
        if "s3.path-style-access" in s3_properties:
            s3_properties["s3.path-style-access"] = "true" if str(s3_properties["s3.path-style-access"]).lower() == "true" else "false"
            
        if "fs.s3a.path.style.access" in hadoop_properties:
            hadoop_properties["fs.s3a.path.style.access"] = "true" if str(hadoop_properties["fs.s3a.path.style.access"]).lower() == "true" else "false"