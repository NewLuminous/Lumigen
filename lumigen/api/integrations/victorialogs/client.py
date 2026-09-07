import typing as t
import json
import logging
import requests
from datetime import datetime

from api.config import settings

logger = logging.getLogger(__name__)

MISSING_MSG_PREFIX = "missing _msg field;"


class VictoriaLogsClient:
    """
    Generic HTTP client for VictoriaLogs.
    Executes LogsQL queries and returns parsed records.
    """
    def __init__(self, base_url: t.Optional[str] = None):
        self.base_url = (base_url or settings.VICTORIA_LOGS_URL or "").rstrip("/")

    @property
    def is_configured(self) -> bool:
        return bool(self.base_url)

    def query_logs(
        self,
        filter_query: str,
        tail: t.Optional[int] = None,
        start_time: t.Optional[datetime] = None,
        end_time: t.Optional[datetime] = None,
        fields: t.Optional[t.List[str]] = None,
    ) -> t.Optional[t.List[t.Dict[str, t.Any]]]:
        """
        Takes a filter expression (e.g. stream or label selector) and returns parsed JSON records.
        """
        if not self.is_configured:
            return None

        target_fields = fields or ["_time", "_msg"]
        query_parts = [
            filter_query.strip(),
            f"| fields {', '.join(target_fields)}",
        ]
        if tail is not None:
            # Forces VictoriaLogs to scan all blocks to avoid the premature cutoff bug.
            query_parts.append(f"| sort by (_time) desc")
        logsql_query = " ".join(query_parts)

        payload: t.Dict[str, t.Any] = {"query": logsql_query}
        if start_time is not None:
            payload["start"] = start_time.isoformat()
        if end_time is not None:
            payload["end"] = end_time.isoformat()

        query_url = f"{self.base_url}/select/logsql/query"
        try:
            logger.debug(f"Executing VictoriaLogs query: {logsql_query} with payload: {payload}")
            with requests.post(query_url, data=payload, timeout=15, stream=True) as response:
                if not response.ok:
                    logger.warning(f"VictoriaLogs query failed with status {response.status_code}: {response.text}")
                    return None

                content_type = response.headers.get("Content-Type", "").lower()
                if "text/html" in content_type:
                    logger.warning(f"VictoriaLogs response returned unexpected Content-Type '{content_type}' (likely an HTML page).")
                    return None

                records: t.List[t.Dict] = []
                first_line = True
                raw_line: str
                for raw_line in response.iter_lines(decode_unicode=True):
                    if not raw_line:
                        continue

                    line = raw_line.decode("utf-8", errors="replace") if isinstance(raw_line, bytes) else raw_line
                    line = line.strip()
                    if not line:
                        continue

                    if first_line:
                        first_line = False
                        if line.startswith(("<", "<!doctype", "<!DOCTYPE", "<html", "<HTML")):
                            return None

                    try:
                        record = json.loads(line)
                        if isinstance(record, dict) and "_msg" in record:
                            msg: str = record.get("_msg", "")
                            if msg.startswith(MISSING_MSG_PREFIX):
                                record["_msg"] = ""

                            records.append(record)
                            if tail is not None and len(records) >= tail:
                                break
                    except json.JSONDecodeError:
                        continue

            if not records:
                return None

            # Sort chronologically (oldest -> newest)
            records.sort(key=lambda r: r.get("_time", ""))

            return records
            
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP request to VictoriaLogs failed: {e}")
            return None