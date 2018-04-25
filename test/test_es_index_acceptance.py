import unittest
from es_python.es_index import EsIndex
import requests


class TestEsIndexAcceptance(unittest.TestCase):

    def setUp(self):
        self.index_name = "tweet"
        self.ei = EsIndex(self.index_name)
        self.id_field = "tweet_id"
        self.test_mapping = {
            "properties": {
                self.id_field: {
                    "type": "integer"
                },
                "user_id": {
                    "type": "integer"
                },
                "body": {
                    "type": "text"
                }
            }
        }
        self.test_data = [
            {
                "tweet_id": 1,
                "user_id": 2,
                "body": "Hello World"
            },
            {
                "tweet_id": 2,
                "user_id": 4,
                "body": "What's up"
            }]
        if self.ei.exists():
            self.ei.delete()
        self.ei.create()

    def test_exists(self):
        self.assertTrue(self.ei.exists())
        self.ei.delete()
        self.assertFalse(self.ei.exists())

    def test_create(self):
        with self.assertRaises(requests.HTTPError):
            self.ei.create()

    def test_delete(self):
        self.ei.delete()
        with self.assertRaises(requests.HTTPError):
            self.ei.delete()

    def test_put_mapping(self):
        self.ei.put_mapping(self.test_mapping)

    def test_get_mapping(self):
        self.ei.put_mapping(self.test_mapping)
        expected = {
            self.index_name:
                {
                    "mappings": {
                        "_doc": self.test_mapping
                    }
                }
        }
        self.assertDictEqual(expected, self.ei.get_mapping())

    def test_bulk_index(self):
        self.ei.bulk_index(docs=self.test_data, id_field=self.id_field)

    def tearDown(self):
        if self.ei.exists():
            self.ei.delete()


if __name__ == '__main__':
    unittest.main()
