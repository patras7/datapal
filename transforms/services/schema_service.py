import json
import urllib.parse

import requests


class SchemaService:
    def __init__(self, uri: str):
        self._uri = uri

    def get_schema(self, auth_token, dataset_rid, branch_id):
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "bellhop-authoring",
            "Authorization": auth_token,
        }
        url = f"{self._uri}/schemas/datasets/{dataset_rid}/branches/{urllib.parse.quote(branch_id, safe='')}"
        response = requests.request("GET", url, headers=headers)
        if response:
            return response.json()

    def put_schema(self, auth_token, dataset_rid, branch_id, schema):
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "bellhop-authoring",
            "Authorization": auth_token,
        }
        url = f"{self._uri}/schemas/datasets/{dataset_rid}/branches/{urllib.parse.quote(branch_id, safe='')}"
        requests.request("POST", url, headers=headers, data=json.dumps(schema))
