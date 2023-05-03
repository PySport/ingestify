# Wyscout demo

## API Credentials

Credentials are specified in the `config_local.yaml`. For safety reasons we use environment variables to pass the actual credentials to `ingestify`.

Create a `.env` file with this content:
```bash
WYSCOUT_USERNAME=<your username>
WYSCOUT_PASSWORD=<your password>
```

The `.env` file is automatically read by `ingestify`.

## Ingestion of data

To sync all the available data from Wyscout to your local system you have to run a command in your shell. This command will read the information from the config and start syncing data.

Make sure the parent directory exists (`/tmp/ingestify-demo/`) 


```bash
ingestify run --config config_local.yaml
```

## Query the data

See `query.py` for a demo.