import unittest
import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import sys
import os

# Add package dir to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from niodb import NioDB, NioDBError


class MockNioDBHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()

        if "/manifest" in self.path:
            self.wfile.write(json.dumps({"session_id": "s1", "manifest_text": "Manifest ok"}).encode("utf-8"))
        elif "/dead-ends" in self.path:
            self.wfile.write(json.dumps({"session_id": "s1", "dead_ends": [{"hypothesis": "test"}], "count": 1}).encode("utf-8"))
        elif self.path.startswith("/api/v1/sessions"):
            self.wfile.write(json.dumps({"items": [{"id": "s1"}], "count": 1}).encode("utf-8"))
        elif self.path.startswith("/api/v1/tasks/task_1"):
            self.wfile.write(json.dumps({"id": "task_1", "status": "pending"}).encode("utf-8"))
        elif self.path.startswith("/api/v1/tasks"):
            self.wfile.write(json.dumps({"items": [{"id": "task_1"}], "count": 1}).encode("utf-8"))
        elif self.path.startswith("/api/v1/agent/tools"):
            self.wfile.write(json.dumps({"tools": [{"name": "query_database"}]}).encode("utf-8"))
        elif self.path.startswith("/api/v1/records/rec_1"):
            self.wfile.write(json.dumps({"id": "rec_1", "data": {"key": "value"}}).encode("utf-8"))
        elif self.path.startswith("/api/v1/records"):
            self.wfile.write(json.dumps({"items": [{"id": "rec_1"}], "count": 1}).encode("utf-8"))
        else:
            self.wfile.write(json.dumps({"status": "ok"}).encode("utf-8"))

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length).decode("utf-8")) if length > 0 else {}

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()

        if self.path.startswith("/api/v1/records/search"):
            self.wfile.write(json.dumps({"items": [{"id": "rec_1", "score": 0.99}], "count": 1}).encode("utf-8"))
        elif self.path.startswith("/api/v1/records/bulk"):
            act = body.get("action", "insert")
            recs = body.get("records", [])
            ids = body.get("ids", [])
            self.wfile.write(json.dumps({
                "success": True,
                "inserted": len(recs) if act == "insert" else 0,
                "updated": len(recs) if act == "update" else 0,
                "deleted": len(ids) if act == "delete" else 0,
                "records": recs,
                "deleted_ids": ids
            }).encode("utf-8"))
        elif self.path.startswith("/api/v1/records"):
            self.wfile.write(json.dumps({"id": "rec_new", **body}).encode("utf-8"))
        elif "/turns" in self.path:
            self.wfile.write(json.dumps({"success": True, "session_id": "s1", "turn": body}).encode("utf-8"))
        elif "/dead-ends" in self.path:
            self.wfile.write(json.dumps({"success": True, "count": 1}).encode("utf-8"))
        elif self.path.startswith("/api/v1/sessions"):
            self.wfile.write(json.dumps({"id": "s1", "title": body.get("title", "")}).encode("utf-8"))
        elif self.path.startswith("/api/v1/tasks"):
            self.wfile.write(json.dumps({"id": "task_1", **body}).encode("utf-8"))
        elif self.path.startswith("/api/v1/admin/vacuum"):
            self.wfile.write(json.dumps({"success": True, "stats": {"compacted_bytes": 500}}).encode("utf-8"))
        elif self.path.startswith("/api/v1/query"):
            self.wfile.write(json.dumps({"items": [{"cnt": 10}]}).encode("utf-8"))
        else:
            self.wfile.write(json.dumps({"status": "ok"}).encode("utf-8"))

    def do_PATCH(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length).decode("utf-8")) if length > 0 else {}

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"id": "task_1", **body}).encode("utf-8"))

    def do_DELETE(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"success": True, "deleted": True, "id": "rec_1"}).encode("utf-8"))

    def log_message(self, format, *args):
        pass  # Quiet logger


class TestNioDBPy(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = HTTPServer(("127.0.0.1", 0), MockNioDBHandler)
        cls.port = cls.server.server_port
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()
        cls.client = NioDB(f"http://127.0.0.1:{cls.port}", token="tok-123", workspace_id="ws-py")

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def test_collection_crud_and_vector(self):
        c_comb = NioDB("http://127.0.0.1:7432", client_token="cli-py", secret_token="sec-py")
        self.assertEqual(c_comb.token, "cli-py:sec-py")

        col = self.client.collection("memories")
        inserted = col.insert({"text": "python test"}, ttl=300)
        self.assertEqual(inserted["id"], "rec_new")
        self.assertEqual(inserted["data"]["ttl"], 300)

        found = col.find(limit=5)
        self.assertEqual(found["count"], 1)

        rec = col.get("rec_1")
        self.assertEqual(rec["id"], "rec_1")

        updated = col.update("rec_1", {"key": "val2"})
        self.assertEqual(updated["id"], "task_1")

        deleted = col.delete("rec_1")
        self.assertTrue(deleted["success"])

        bulk_ins = col.insert_many([{"text": "a"}, {"text": "b"}])
        self.assertEqual(bulk_ins["inserted"], 2)

        bulk_upd = col.update_many([{"id": "rec_1", "text": "a2"}])
        self.assertEqual(bulk_upd["updated"], 1)

        bulk_del = col.delete_many(["rec_1", "rec_2"])
        self.assertEqual(bulk_del["deleted"], 2)

        bulk_all = self.client.bulk([{"action": "insert", "collection": "test", "data": {}}])
        self.assertTrue(bulk_all["success"])

        search_res = col.search_vector([0.1, 0.2, 0.3], top_k=3)
        self.assertEqual(search_res["count"], 1)
        self.assertEqual(search_res["items"][0]["score"], 0.99)

    def test_sessions_and_dead_ends(self):
        sess = self.client.sessions.create("Py Task", goal="Test python client")
        self.assertEqual(sess["id"], "s1")

        sessions_list = self.client.sessions.list()
        self.assertEqual(sessions_list["count"], 1)

        s1 = self.client.session("s1")
        turn_res = s1.append_turn(agent="worker", model="gpt-4o", summary="Completed step")
        self.assertTrue(turn_res["success"])

        manifest = s1.get_manifest()
        self.assertEqual(manifest["manifest_text"], "Manifest ok")

        dead_end_res = s1.log_dead_end(hypothesis="hypothesis 1", reason="failed validation")
        self.assertTrue(dead_end_res["success"])

        dead_ends = s1.get_dead_ends()
        self.assertEqual(dead_ends["count"], 1)

    def test_tasks_and_admin(self):
        task = self.client.tasks.create("Run py workflow", prompt="Execute job")
        self.assertEqual(task["id"], "task_1")

        tasks_list = self.client.tasks.list()
        self.assertEqual(tasks_list["count"], 1)

        updated = self.client.tasks.update("task_1", status="running", progress=75)
        self.assertEqual(updated["status"], "running")
        self.assertEqual(updated["progress"], 75)

        query_res = self.client.query("SELECT COUNT(*) FROM artifacts")
        self.assertEqual(query_res["items"][0]["cnt"], 10)

        vac_res = self.client.vacuum()
        self.assertTrue(vac_res["success"])

        tools = self.client.get_tools()
        self.assertEqual(len(tools["tools"]), 1)


if __name__ == "__main__":
    unittest.main()
