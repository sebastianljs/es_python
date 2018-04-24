import requests
import os


class EsManager(object):
    def __init__(self,  http_port="localhost:9200"):
        self.http_port = http_port

    def create_index(self, name: str, n_shards: int=5, n_replicas: int=1):
        """
        Creates an ES index
        :param name: Index name
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
        create_index_req = requests.post(
            url="{http_port}/{name}".format(
                http_port=self.http_port,
                name=name),
            params=req_params)
        create_index_req.raise_for_status()

    def delete_index(self, name: str):
        """
        Deletes an ES index
        :param name:
        :return:
        """
        delete_index_req = requests.delete(
            url="{http_port}/{name}".format(
                http_port=self.http_port,
                name=name
            )
        )
        delete_index_req.raise_for_status()

