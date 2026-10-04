"""
NioDB Python Client SDK — Zero-dependency Python wrapper for NioDB.
"""

import json
import urllib.request
import urllib.error
from typing import Any, Dict, List, Optional, Union

class NioDBError(Exception):
    def __init__(self, message: str, status_code: Optional[int] = None, data: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.status_code = status_code
        self.data = data


class Collection:
    def __init__(self, client: "NioDB", name: str):
        self._client = client
        self.name = name

    def insert(self, data: Dict[str, Any], ttl: Optional[int] = None, idempotency_key: Optional[str] = None) -> Dict[str, Any]:
        payload = {"collection": self.name, "data": dict(data)}
        if ttl is not None:
            payload["data"]["ttl"] = ttl
        headers = {}
        if idempotency_key:
            headers["idempotency-key"] = idempotency_key
        return self._client._request("/api/v1/records", method="POST", body=payload, extra_headers=headers)

    def find(self, limit: Optional[int] = None, cursor: Optional[str] = None) -> Dict[str, Any]:
        path = f"/api/v1/records?type={urllib.parse.quote(self.name)}"
        if limit is not None:
            path += f"&limit={limit}"
        if cursor is not None:
            path += f"&cursor={urllib.parse.quote(cursor)}"
        return self._client._request(path, method="GET")

    def get(self, record_id: str) -> Dict[str, Any]:
        return self._client._request(f"/api/v1/records/{urllib.parse.quote(record_id)}", method="GET")

    def update(self, record_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        payload = data if "data" in data else {"data": data}
        return self._client._request(f"/api/v1/records/{urllib.parse.quote(record_id)}", method="PATCH", body=payload)

    def delete(self, record_id: str) -> Dict[str, Any]:
        return self._client._request(f"/api/v1/records/{urllib.parse.quote(record_id)}", method="DELETE")

    def insert_many(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        formatted = []
        for r in records:
            if "data" in r:
                formatted.append(r)
            else:
                formatted.append({"collection": self.name, "data": r})
        payload = {
            "collection": self.name,
            "action": "insert",
            "records": formatted
        }
        return self._client._request("/api/v1/records/bulk", method="POST", body=payload)

    def bulk_insert(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        return self.insert_many(records)

    def update_many(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        formatted = []
        for r in records:
            rec_id = r.get("id") or r.get("record_id")
            data = r.get("data") if "data" in r else r
            formatted.append({"id": rec_id, "data": data})
        payload = {
            "action": "update",
            "records": formatted
        }
        return self._client._request("/api/v1/records/bulk", method="POST", body=payload)

    def bulk_update(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        return self.update_many(records)

    def delete_many(self, ids: List[Union[str, Dict[str, Any]]]) -> Dict[str, Any]:
        formatted_ids = []
        for item in ids:
            if isinstance(item, dict):
                rec_id = item.get("id") or item.get("record_id")
                if rec_id:
                    formatted_ids.append(rec_id)
            else:
                formatted_ids.append(str(item))
        payload = {
            "action": "delete",
            "ids": formatted_ids
        }
        return self._client._request("/api/v1/records/bulk", method="POST", body=payload)

    def bulk_delete(self, ids: List[Union[str, Dict[str, Any]]]) -> Dict[str, Any]:
        return self.delete_many(ids)

    def search_vector(self, vector: List[float], top_k: int = 5, min_score: float = 0.0) -> Dict[str, Any]:
        payload = {
            "collection": self.name,
            "vector": vector,
            "top_k": top_k,
            "min_score": min_score
        }
        return self._client._request("/api/v1/records/search", method="POST", body=payload)


class Session:
    def __init__(self, client: "NioDB", session_id: str):
        self._client = client
        self.session_id = session_id

    def create(self, title: str, goal: str = "", model_tier: str = "strong", metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload = {
            "title": title,
            "goal": goal,
            "model_tier": model_tier,
            "metadata": metadata or {}
        }
        return self._client._request("/api/v1/sessions", method="POST", body=payload)

    def get(self) -> Dict[str, Any]:
        return self._client._request(f"/api/v1/sessions/{urllib.parse.quote(self.session_id)}", method="GET")

    def append_turn(
        self,
        agent: str,
        model: str,
        summary: str = "",
        files_touched: Optional[List[str]] = None,
        tokens: Optional[Dict[str, Any]] = None,
        cost_usd: float = 0.0
    ) -> Dict[str, Any]:
        payload = {
            "agent": agent,
            "model": model,
            "summary": summary,
            "files_touched": files_touched or [],
            "tokens": tokens,
            "cost_usd": cost_usd
        }
        return self._client._request(f"/api/v1/sessions/{urllib.parse.quote(self.session_id)}/turns", method="POST", body=payload)

    def get_manifest(self) -> Dict[str, Any]:
        return self._client._request(f"/api/v1/sessions/{urllib.parse.quote(self.session_id)}/manifest", method="GET")

    def log_dead_end(self, hypothesis: str, reason: str, agent: str = "unknown") -> Dict[str, Any]:
        payload = {
            "hypothesis": hypothesis,
            "reason": reason,
            "agent": agent
        }
        return self._client._request(f"/api/v1/sessions/{urllib.parse.quote(self.session_id)}/dead-ends", method="POST", body=payload)

    def get_dead_ends(self) -> Dict[str, Any]:
        return self._client._request(f"/api/v1/sessions/{urllib.parse.quote(self.session_id)}/dead-ends", method="GET")


class Sessions:
    def __init__(self, client: "NioDB"):
        self._client = client

    def list(self) -> Dict[str, Any]:
        return self._client._request("/api/v1/sessions", method="GET")

    def create(self, title: str, goal: str = "", model_tier: str = "strong", metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload = {
            "title": title,
            "goal": goal,
            "model_tier": model_tier,
            "metadata": metadata or {}
        }
        return self._client._request("/api/v1/sessions", method="POST", body=payload)

    def get(self, session_id: str) -> Session:
        return Session(self._client, session_id)


class Tasks:
    def __init__(self, client: "NioDB"):
        self._client = client

    def list(self) -> Dict[str, Any]:
        return self._client._request("/api/v1/tasks", method="GET")

    def create(self, title: str, prompt: str, priority: str = "normal", metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload = {
            "title": title,
            "prompt": prompt,
            "priority": priority,
            "metadata": metadata or {}
        }
        return self._client._request("/api/v1/tasks", method="POST", body=payload)

    def get(self, task_id: str) -> Dict[str, Any]:
        return self._client._request(f"/api/v1/tasks/{urllib.parse.quote(task_id)}", method="GET")

    def update(self, task_id: str, status: Optional[str] = None, progress: Optional[int] = None, log_line: Optional[str] = None, result: Any = None) -> Dict[str, Any]:
        payload = {}
        if status is not None: payload["status"] = status
        if progress is not None: payload["progress"] = progress
        if log_line is not None: payload["log_line"] = log_line
        if result is not None: payload["result"] = result
        return self._client._request(f"/api/v1/tasks/{urllib.parse.quote(task_id)}", method="PATCH", body=payload)


class NioDB:
    def __init__(self, url: str = "http://127.0.0.1:7432", token: Optional[str] = None, client_token: Optional[str] = None, secret_token: Optional[str] = None, workspace_id: Optional[str] = None):
        self.url = url.rstrip("/")
        if token:
            self.token = token
        elif client_token and secret_token:
            self.token = f"{client_token}:{secret_token}"
        elif client_token or secret_token:
            self.token = client_token or secret_token
        else:
            self.token = None
        self.workspace_id = workspace_id
        self.sessions = Sessions(self)
        self.tasks = Tasks(self)

    def _request(self, path: str, method: str = "GET", body: Optional[Dict[str, Any]] = None, extra_headers: Optional[Dict[str, str]] = None) -> Any:
        url = f"{self.url}{path}"
        if self.workspace_id and "workspace_id=" not in url:
            sep = "&" if "?" in url else "?"
            url += f"{sep}workspace_id={urllib.parse.quote(self.workspace_id)}"

        headers = {
            "Accept": "application/json",
            "User-Agent": "nio-db-py/0.1.1"
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if extra_headers:
            headers.update(extra_headers)

        data_bytes = None
        if body is not None and method != "GET":
            headers["Content-Type"] = "application/json"
            data_bytes = json.dumps(body).encode("utf-8")

        req = urllib.request.Request(url, data=data_bytes, headers=headers, method=method)

        try:
            with urllib.request.urlopen(req) as resp:
                if resp.status == 204:
                    return None
                resp_bytes = resp.read()
                if not resp_bytes:
                    return {}
                return json.loads(resp_bytes.decode("utf-8"))
        except urllib.error.HTTPError as e:
            try:
                err_data = json.loads(e.read().decode("utf-8"))
                msg = err_data.get("error", {}).get("message", str(e))
            except Exception:
                err_data = None
                msg = str(e)
            raise NioDBError(msg, status_code=e.code, data=err_data) from e
        except urllib.error.URLError as e:
            raise NioDBError(f"Connection failed: {e}") from e

    def collection(self, name: str) -> Collection:
        return Collection(self, name)

    def session(self, session_id: str) -> Session:
        return Session(self, session_id)

    def bulk(self, operations: Union[List[Dict[str, Any]], Dict[str, Any]]) -> Dict[str, Any]:
        if isinstance(operations, list):
            payload = {"operations": operations}
        else:
            payload = operations
        return self._request("/api/v1/records/bulk", method="POST", body=payload)

    def query(self, sql: str) -> Dict[str, Any]:
        return self._request("/api/v1/query", method="POST", body={"sql": sql})

    def vacuum(self) -> Dict[str, Any]:
        return self._request("/api/v1/admin/vacuum", method="POST", body={})

    def get_tools(self) -> Dict[str, Any]:
        return self._request("/api/v1/agent/tools", method="GET")
