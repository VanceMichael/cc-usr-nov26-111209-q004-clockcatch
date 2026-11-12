import unittest
from pathlib import Path
from clockcatch.parser import inspect_document

class ParserTests(unittest.TestCase):
    def test_tick_range(self):
        result = inspect_document(Path('clockcatch/fixtures/samples.json').read_bytes())
        self.assertEqual((result['minimum_tick'], result['maximum_tick']), (0, 100))
        self.assertEqual(result['sample_count'], 11)

    def test_empty(self):
        result = inspect_document(b'{"bootId":"B","samples":[]}')
        self.assertIsNone(result['minimum_tick'])

    def test_negative_and_boolean(self):
        for value in [b'-1', b'true']:
            with self.subTest(value=value), self.assertRaises(ValueError):
                inspect_document(b'{"bootId":"A","samples":[{"tick":' + value + b'}]}')

    def test_boot_required(self):
        with self.assertRaises(ValueError):
            inspect_document(b'{"samples":[]}')
