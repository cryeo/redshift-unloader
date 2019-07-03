# redshift-unloader
Unloads the result of a query on Amazon Redshift to local storage.

## Prerequisites
- Python 3.6+
- boto3 1.7.84
- psycopg2 2.7.5
- psycopg2-binary 2.7.5

## Installation
The package is available on PyPI:

```bash
pip install redshift-unloader
```

### Usage
Unloaded data is supposed to be gzipped.

```py
from redshift_unloader import RedshiftUnloader

ru = RedshiftUnloader(host='<redshift host>',
                      port=<redshift port>,
                      user='<redshift user>',
                      password='<redshift password>',
                      database='<redshift database name>',
                      s3_bucket='<s3 bucket name>',
                      access_key_id='<aws access key id>',
                      secret_access_key='<aws secret access key>',
                      region='<aws region>',
                      verbose=False)

ru.unload("SELECT * FROM my_table WHERE log_time >= 'yyyyMMdd'",
          "/path/to/result.csv.gz",
          delimiter=',',
          add_quotes=True,
          escape=True,
          null_string='',
          with_header=True)
```
