import base64
import json
import threading
import unittest
import urllib.parse
import urllib.request

from clockcatch.source_server import FixtureServer
from clockcatch.transport import fetch


class SourceTests(unittest.TestCase):
    def setUp(self):
        self.server = FixtureServer()
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = 'http://127.0.0.1:' + str(self.server.server_address[1])

    def tearDown(self):
        self.server.shutdown()
        self.thread.join(timeout=2)
        self.server.server_close()

    def get(self, path):
        return fetch(self.base + path)

    def phase(self, value):
        self.assertEqual(self.get('/control/phase?value=' + value)[0], 200)

    def test_interval_truncation_and_boot_identity(self):
        result = json.loads(self.get('/samples?boot=A&from=0&until=100')[2])
        self.assertTrue(result['truncated'])
        self.assertEqual([row['tick'] for row in result['samples']], [0, 10, 20])
        result = json.loads(self.get('/samples?boot=B&from=0&until=40')[2])
        self.assertEqual(result['bootId'], 'B')
        self.assertEqual([row['tick'] for row in result['samples']], [0, 20, 40])

    def test_anchor_revision(self):
        first = json.loads(self.get('/anchors')[2])
        self.phase('complete')
        second = json.loads(self.get('/anchors')[2])
        self.phase('revised')
        third = json.loads(self.get('/anchors')[2])
        self.assertEqual(len(first['anchors']), 2)
        self.assertEqual(len(second['anchors']), 4)
        self.assertNotEqual(second['anchors'][1]['utc_us'], third['anchors'][1]['utc_us'])
