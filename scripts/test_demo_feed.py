import os
import tempfile
import unittest

import demo_feed


class FakeServer:
    """Answers the demo's requests and writes delivered rows like a delivery run."""

    def __init__(self, demo_dir, feeds_status=200, deliver=True, existing=False, event_status=202):
        self.demo_dir = demo_dir
        self.feeds_status = feeds_status
        self.deliver = deliver
        self.existing = existing
        self.event_status = event_status
        self.calls = []

    def __call__(self, method, path, body=None, token=None):
        self.calls.append((method, path, token))
        if (method, path) == ("GET", "/v1/feeds"):
            return self.feeds_status, {}
        if method == "GET":
            return (200 if self.existing else 404), {}
        if path == "/v1/templates":
            return 201, {"id": body["id"], "version": 1}
        if path == "/v1/feeds":
            return 201, {"name": body["name"]}
        if path.endswith("/tokens"):
            return 201, {"token": "fdt_demo"}
        if path.endswith("/events"):
            if self.event_status == 202 and self.deliver:
                output = os.path.join(self.demo_dir, demo_feed.OUTPUT)
                os.makedirs(os.path.dirname(output), exist_ok=True)
                with open(output, "a", encoding="utf-8") as handle:
                    handle.write('{"order_id": %d}\n' % body["order_id"])
            return self.event_status, {}
        return 500, {}


class SeedTest(unittest.TestCase):
    def seed(self, server, demo_dir):
        lines = []
        ticks = iter(range(0, 10_000, 30))
        code = demo_feed.seed(
            "http://x", demo_dir, call=server, sleep=lambda _: None,
            clock=lambda: next(ticks), log=lines.append, stamp=1000,
        )
        return code, lines

    def test_registers_creates_sends_and_sees_the_rows(self):
        with tempfile.TemporaryDirectory() as demo_dir:
            server = FakeServer(demo_dir)
            code, lines = self.seed(server, demo_dir)
            self.assertEqual(code, 0, lines)
            posts = [path for method, path, _ in server.calls if method == "POST"]
            self.assertEqual(
                posts,
                ["/v1/templates", "/v1/feeds", "/v1/feeds/demo-orders/tokens"]
                + ["/v1/feeds/demo-orders/events"] * demo_feed.EVENTS,
            )
            sent_with = {token for _, path, token in server.calls if path.endswith("/events")}
            self.assertEqual(sent_with, {"fdt_demo"})
            self.assertEqual(demo_feed.line_count(os.path.join(demo_dir, demo_feed.OUTPUT)), 3)
            self.assertTrue(any("3 events posted" in line for line in lines))

    def test_existing_template_and_feed_are_left_alone(self):
        with tempfile.TemporaryDirectory() as demo_dir:
            output = os.path.join(demo_dir, demo_feed.OUTPUT)
            os.makedirs(os.path.dirname(output))
            with open(output, "w", encoding="utf-8") as handle:
                handle.write('{"order_id": 1}\n')
            server = FakeServer(demo_dir, existing=True)
            code, lines = self.seed(server, demo_dir)
            self.assertEqual(code, 0, lines)
            posts = [path for method, path, _ in server.calls if method == "POST"]
            self.assertNotIn("/v1/templates", posts)
            self.assertNotIn("/v1/feeds", posts)
            self.assertEqual(demo_feed.line_count(output), 4)

    def test_a_build_without_feeds_is_skipped(self):
        with tempfile.TemporaryDirectory() as demo_dir:
            code, lines = self.seed(FakeServer(demo_dir, feeds_status=404), demo_dir)
            self.assertEqual(code, 0)
            self.assertTrue(any("skipped" in line for line in lines))

    def test_an_unreachable_server_fails(self):
        with tempfile.TemporaryDirectory() as demo_dir:
            code, lines = self.seed(FakeServer(demo_dir, feeds_status=0), demo_dir)
            self.assertEqual(code, 1)

    def test_a_refused_event_fails(self):
        with tempfile.TemporaryDirectory() as demo_dir:
            code, lines = self.seed(FakeServer(demo_dir, event_status=401), demo_dir)
            self.assertEqual(code, 1)
            self.assertTrue(any("answered 401" in line for line in lines))

    def test_events_never_delivered_fail_after_the_wait(self):
        with tempfile.TemporaryDirectory() as demo_dir:
            code, lines = self.seed(FakeServer(demo_dir, deliver=False), demo_dir)
            self.assertEqual(code, 1)
            self.assertTrue(any("gained 0 rows" in line for line in lines))


class HelpersTest(unittest.TestCase):
    def test_line_count_ignores_blank_lines_and_missing_files(self):
        with tempfile.TemporaryDirectory() as demo_dir:
            path = os.path.join(demo_dir, "rows.jsonl")
            self.assertEqual(demo_feed.line_count(path), 0)
            with open(path, "w", encoding="utf-8") as handle:
                handle.write('{"a": 1}\n\n{"a": 2}\n')
            self.assertEqual(demo_feed.line_count(path), 2)

    def test_request_reports_an_unreachable_server(self):
        status, _ = demo_feed.request("http://127.0.0.1:1", "GET", "/v1/feeds")
        self.assertEqual(status, 0)

    def test_template_and_feed_refusals_are_reported(self):
        refuse = lambda method, path, body=None, token=None: (404, {}) if method == "GET" else (422, "no")
        self.assertIn("422", demo_feed.ensure_template(refuse))
        self.assertIn("422", demo_feed.ensure_feed(refuse))
        token, problem = demo_feed.issue_token(refuse, "x")
        self.assertIsNone(token)
        self.assertIn("422", problem)


if __name__ == "__main__":
    unittest.main()
