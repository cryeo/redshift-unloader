import psycopg2
import psycopg2.extensions
import re

from typing import Dict, List, Optional

from redshift_unloader.credential import Credential
from redshift_unloader.logger import logger


class Redshift:
    __credential: Credential
    __connection: psycopg2.extensions.connection
    __cursor: psycopg2.extensions.cursor

    def __init__(self, host: str, port: int, user: str, password: str,
                 database: str, credential: Credential) -> None:
        self.__credential = credential
        self.__connection = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database)
        self.__connection.autocommit = True
        self.__cursor = self.__connection.cursor()

    def __del__(self) -> None:
        try:
            self.__connection.close()
            self.__cursor.close()
        except:
            pass

    def get_columns(self, query: str, add_quotes: bool = True) -> List[str]:
        quote = '"' if add_quotes else ''
        sql = self.__generate_get_columns_sql(query)
        logger.debug("query: %s", sql)

        try:
            self.__cursor.execute(sql)
            result = [f'{quote}{column.name}{quote}' for column in self.__cursor.description]

            return result
        except Exception as e:
            raise e

    def unload(self,
               query: str,
               s3_uri: str,
               manifest: bool = False,
               delimiter: Optional[str] = None,
               fixed_width: Optional[str] = None,
               encrypted: bool = None,
               gzip: bool = False,
               add_quotes: bool = False,
               null_string: Optional[str] = None,
               escape: bool = False,
               allow_overwrite: bool = False,
               parallel: bool = True,
               max_file_size: Optional[str] = None) -> bool:
        options: Dict[str, Optional[str]] = {}

        if manifest:
            options['MANIFEST'] = None
        if delimiter is not None:
            options['DELIMITER'] = f"'{delimiter}'"
        if fixed_width is not None:
            options['FIXEDWIDTH'] = f"'fixed_width'"
        if encrypted:
            options['ENCRYPTED'] = None
        if gzip:
            options['GZIP'] = None
        if add_quotes:
            options['ADDQUOTES'] = None
        if null_string is not None:
            options['NULL'] = f"'{null_string}'"
        if escape:
            options['ESCAPE'] = None
        if allow_overwrite:
            options['ALLOWOVERWRITE'] = None
        options['PARALLEL'] = 'ON' if parallel else 'OFF'
        if max_file_size is not None:
            options['MAXFILESIZE'] = max_file_size

        sql = self.__generate_unload_sql(self.__escaped_query(query), s3_uri, self.__credential, options)
        logger.debug("query: %s", sql)

        try:
            self.__cursor.execute(sql)
            return True
        except Exception as e:
            raise e

    @staticmethod
    def __escaped_query(query: str) -> str:
        return re.sub(r'[\\\']', lambda x: '\\' + x.group(), query)

    @staticmethod
    def __generate_get_columns_sql(query: str) -> str:
        return f'WITH query AS ({query}) SELECT * FROM query LIMIT 0'

    @staticmethod
    def __generate_unload_sql(query: str, s3_uri: str, credential: Credential, options: Dict) -> str:
        partial_sqls = [f"UNLOAD ('{query}') TO '{s3_uri}'"]
        partial_sqls.extend([f"{k} '{v}'" for (k, v) in credential.to_dict().items()])
        partial_sqls.extend([f"{k} {v}" if v is not None else f"{k}" for (k, v) in options.items()])

        return ' '.join(partial_sqls)
