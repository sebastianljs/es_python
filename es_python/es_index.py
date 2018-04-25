import requests
import json
from pprint import pprint


class EsIndex(object):
    def __init__(self, name: str, http_port="http://localhost:9200"):
        self.name = name
        self.http_port = http_port

    def exists(self):
        """
        Check if the ES index exists
        :return:
        """
        head_req = requests.head(
            url="{http_port}/{name}".format(
                http_port=self.http_port,
                name=self.name
            )
        )
        return head_req.status_code == 200

    def create(self, n_shards: int=5, n_replicas: int=1):
        """
        Creates the ES index
        :param n_shards: Number of shards for index
        :param n_replicas: Number of replicas for index
        :return:
        """
        req_params = {
            "settings": {
                "index": {
                    "number_of_shards": n_shards,
                    "number_of_replicas": n_replicas
                }
            }
        }
        create_index_req = requests.put(
            url="{http_port}/{name}".format(
                http_port=self.http_port,
                name=self.name),
            headers={"Content-Type": "application/json"},
            data=json.dumps(req_params))
        create_index_req.raise_for_status()

    def delete(self):
        """
        Deletes the ES index
        :return:
        """
        delete_index_req = requests.delete(
            url="{http_port}/{name}".format(
                http_port=self.http_port,
                headers={"Content-Type": "application/json"},
                name=self.name
            )
        )
        delete_index_req.raise_for_status()

    def put_mapping(self, mapping: dict):
        """
        Creates or updates the mapping for the ES index.
        Note: Existing mappings generally cannot be updated. See ES docs
        :param mapping: Mapping for index
        :return:
        """
        put_mapping_req = requests.put(
            url="{http_port}/{name}/_mapping/_doc".format(
                http_port=self.http_port,
                name=self.name
            ),
            headers={"Content-Type": "application/json"},
            data=json.dumps(mapping)
        )
        put_mapping_req.raise_for_status()

    def get_mapping(self):
        get_mapping_req = requests.get(
            url="{http_port}/{name}/_mapping/_doc".format(
                http_port=self.http_port,
                name=self.name
            )
        )
        get_mapping_req.raise_for_status()
        return get_mapping_req.json()

    def bulk_index(self, docs: list, id_field: str):
        """
        Bulk indexes docs
        :param docs: Docs to index
        :param id_field: id field of the ES index
        :return:
        """
        bulk_str = "\n".join([self._make_str(doc, id_field) for doc in docs])
        bulk_str += "\n"  # bulk requests must be terminated by a newline
        bulk_index_req = requests.post(
            url="{http_port}/{name}/_bulk".format(
                http_port=self.http_port,
                name=self.name
            ),
            headers={"Content-Type": "application/json"},
            data=bulk_str
        )
        bulk_index_req.raise_for_status()

    def _make_str(self, doc, id_field: str):
        action_str = json.dumps(
            {
                "index": {"_index": self.name,
                          "_type": "_doc",
                          "_id": doc[id_field]}
            }
        )
        doc_str = json.dumps(
            {
                k: v for k, v in doc.items()
            }
        )
        return "\n".join([action_str, doc_str])
