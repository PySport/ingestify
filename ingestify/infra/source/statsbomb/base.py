from typing import Optional

import requests

from ingestify import Source
from ingestify.exceptions import ConfigurationError


class StatsBombBaseAPI(Source):
    provider = "statsbomb"
    BASE_URL = "https://data.statsbombservices.com/api"

    def __init__(self, name: str, username: str, password: str):
        super().__init__(name)

        self.username = username.strip()
        self.password = password.strip()

        if not self.username:
            raise ConfigurationError(
                f"Username of StatsBomb source named '{self.name}' cannot be empty"
            )

        if not self.password:
            raise ConfigurationError(
                f"Username of StatsBomb source named '{self.name}' cannot be empty"
            )

    def get_url(self, data_feed_key: str, data_spec_version: str, path: str):
        return f"{self.BASE_URL}/{data_spec_version}/{data_feed_key}/{path}"

    def get(self, data_spec_version: str, path: str):
        url = f"{self.BASE_URL}/{data_spec_version}/{path}"
        res = requests.get(url, auth=(self.username, self.password))
        res.raise_for_status()
        return res.json()
