import json
from json import JSONDecodeError
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

from ingestify.exceptions import ConfigurationError


class SecretsManager:
    def __init__(self):
        self._aws_client = None
        self._gcp_client = None

    @property
    def aws_client(self):
        if not self._aws_client:
            self._aws_client = boto3.client("secretsmanager")
        return self._aws_client

    @property
    def gcp_client(self):
        if not self._gcp_client:
            try:
                from google.cloud import secretmanager
            except ImportError as e:
                raise ConfigurationError(
                    "google-cloud-secret-manager is required for vault+gcp:// "
                    "secrets. Install with: pip install google-cloud-secret-manager"
                ) from e
            self._gcp_client = secretmanager.SecretManagerServiceClient()
        return self._gcp_client

    def load_as_dict(self, url: str) -> dict:
        """Load a JSON secret from a supported vault.

        Supported schemes:
            vault+aws://<secret_id>            (AWS Secrets Manager)
            vault+gcp://<project>/<secret>     (GCP Secret Manager)
        """
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

        elif parts.scheme == "vault+gcp":
            project = parts.netloc
            secret_name = parts.path.lstrip("/")
            if not project or not secret_name:
                raise ConfigurationError(
                    f"Invalid GCP secret URL '{url}'. "
                    f"Expected format: vault+gcp://<project>/<secret>"
                )
            name = f"projects/{project}/secrets/{secret_name}/versions/latest"
            try:
                response = self.gcp_client.access_secret_version(request={"name": name})
            except Exception as err:
                raise ConfigurationError(
                    f"Couldn't load GCP secret '{url}': {err}"
                ) from err
            try:
                secrets = json.loads(response.payload.data.decode("utf-8"))
            except JSONDecodeError:
                raise Exception(f"Secret url '{url}' could not be parsed.")

        else:
            raise Exception(f"Secret url '{url}' is not supported.")
        return secrets

    def supports(self, url: str):
        return url.startswith("vault+aws://") or url.startswith("vault+gcp://")

    def load_as_db_url(self, secret_uri: str):
        """Load the secret and return it as a database url."""
        secrets = self.load_as_dict(secret_uri)
        return (
            f"{secrets['engine']}://"
            f"{secrets['username']}:{secrets['password']}"
            f"@{secrets['host']}:{secrets['port']}"
            f"/{secrets['dbname']}"
        )
