import json
from json import JSONDecodeError
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

from ingestify.exceptions import ConfigurationError


class SecretsManager:
    def __init__(self):
        self._aws_client = None

    @property
    def aws_client(self):
        if not self._aws_client:
            self._aws_client = boto3.client("secretsmanager")
        return self._aws_client

    def load_as_dict(self, url: str) -> dict:
        """Load a secret from the supported vault. In this case only AWS Secrets Manager"""
        parts = urlparse(url)
        if parts.scheme == "vault+aws":
            secret_id = parts.netloc + parts.path
            try:
                item = self.aws_client.get_secret_value(SecretId=secret_id)
            except ClientError as err:
                if err.response["Error"]["Code"] == "ResourceNotFoundException":
                    raise ConfigurationError(f"Couldn't find secret: {url}")
                raise

            try:
                secrets = json.loads(item["SecretString"])
            except JSONDecodeError:
                raise Exception(f"Secret url '{url}' could not be parsed.")

        else:
            raise Exception(f"Secret url '{url}' is not supported.")
        return secrets

    def supports(self, url: str):
        return url.startswith("vault+aws://")

    def load_as_db_url(self, secret_uri: str):
        """Load the secret and return it as a database url."""
        secrets = self.load_as_dict(secret_uri)
        return (
            f"{secrets['engine']}://"
            f"{secrets['username']}:{secrets['password']}"
            f"@{secrets['host']}:{secrets['port']}"
            f"/{secrets['dbname']}"
        )
