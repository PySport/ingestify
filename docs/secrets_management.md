# Secrets Management

Ingestify provides robust secrets management capabilities, allowing you to securely store and access sensitive information like API keys, database credentials, and other configuration values.

## Environment Variables

The simplest way to handle secrets is using environment variables through the `!ENV` YAML tag:

```yaml
sources:
  statsbomb:
    type: ingestify.statsbomb_api
    configuration:
      username: !ENV ${STATSBOMB_USERNAME}
      password: !ENV ${STATSBOMB_PASSWORD}
```

Environment variables can have default values:

```yaml
main:
  metadata_url: !ENV ${DATABASE_URL:sqlite:///database/catalog.db}
  file_url: !ENV ${FILE_URL:file://database/files/}
  default_bucket: !ENV ${BUCKET:main}
```

## AWS Secrets Manager Integration

Ingestify supports integration with AWS Secrets Manager for more advanced secrets management. This allows you to store sensitive configuration securely in AWS and reference it in your Ingestify configuration.

### Basic Usage

Reference AWS secrets using the `vault+aws://` protocol:

```yaml
main:
  metadata_url: !ENV vault+aws://path/to/secrets/database

sources:
  statsbomb:
    type: ingestify.statsbomb_api
    configuration: !ENV vault+aws://path/to/secrets/statsbomb
```

### Configuration Structure

When using AWS Secrets Manager, you can store:

1. **Individual secrets**: Referenced directly in your configuration
2. **Secret collections**: Multiple related secrets grouped together

For example, a statsbomb configuration in AWS Secrets Manager might look like:

```json
{
  "username": "api_user",
  "password": "api_password"
}
```

### Secret Path Format

The format for secret paths is:

```
vault+aws://path/to/secret
```

You can also use environment variables in the path:

```yaml
main:
  metadata_url: !ENV vault+aws://project/${ENVIRONMENT:development}/database
```

This will use the `ENVIRONMENT` environment variable, defaulting to "development" if not set.

### Loading Database URLs

For database connections, Ingestify can automatically format the connection string:

```yaml
main:
  metadata_url: !ENV vault+aws://project/production/metadataStore
```

The secret in AWS should contain the database connection components:

```json
{
  "engine": "postgresql",
  "username": "dbuser",
  "password": "dbpassword",
  "host": "database.example.com",
  "port": "5432",
  "dbname": "ingestify"
}
```

Ingestify will construct a proper database URL from these components.

### Loading Configuration Objects

For source configurations, entire configuration objects can be loaded:

```yaml
sources:
  statsbomb:
    type: ingestify.statsbomb_api
    configuration: !ENV vault+aws://project/production/sources/statsbomb
```

This will load the entire JSON object from the AWS secret and use it as the configuration.

### Multiple Secret Sources

You can combine multiple secret sources by providing them as a list:

```yaml
sources:
  statsbomb:
    type: ingestify.statsbomb_api
    configuration:
      - !ENV vault+aws://project/production/sources/statsbomb_credentials
      - !ENV vault+aws://project/production/sources/statsbomb_additional_config
      - api_version: "v4"  # Static configuration can be mixed with secrets
```

Ingestify will merge all these sources into a single configuration object. If there are duplicate keys, later sources in the list will override earlier ones.

## Required AWS Permissions

To use AWS Secrets Manager with Ingestify, your AWS credentials need the following permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": "arn:aws:secretsmanager:*:*:secret:path/to/your/secrets/*"
    }
  ]
}
```

## AWS Credentials

Ingestify uses the standard AWS credential resolution:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. Shared credential file (`~/.aws/credentials`)
3. IAM roles for Amazon EC2 or ECS tasks
4. AWS profile specified by `AWS_PROFILE` environment variable

Make sure your credentials are properly configured before using the AWS Secrets Manager integration.