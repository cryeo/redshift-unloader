import os
import shutil
import tempfile
import uuid
import gzip
import io

from redshift_unloader.credential import Credential
from redshift_unloader.redshift import Redshift
from redshift_unloader.s3 import S3
from redshift_unloader.logger import logger

KB = 1024
MB = 1024 * 1024


class RedshiftUnloader:
    __redshift: Redshift
    __s3: S3
    __credential: Credential

    def __init__(self, host: str, port: int, user: str, password: str,
                 database: str, s3_bucket: str, access_key_id: str,
                 secret_access_key: str, region: str) -> None:
        credential = Credential(
            access_key_id=access_key_id, secret_access_key=secret_access_key)
        self.__redshift = Redshift(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            credential=credential)
        self.__s3 = S3(credential=credential, bucket=s3_bucket, region=region)

    def unload(self, query: str, filename: str, with_header: bool = True) -> None:
        session_id = self.__generate_session_id()
        logger.debug("Session id: %s", session_id)

        s3_path = self.__generate_path("/tmp/redshift-unloader", session_id, '/')
        local_path = self.__generate_path(tempfile.gettempdir(), session_id)

        logger.debug("Get columns")
        columns = self.__redshift.get_columns(query) if with_header else None

        logger.debug("Unload")
        self.__redshift.unload(
            query,
            self.__s3.uri(s3_path),
            gzip=True,
            parallel=True,
            delimiter=',',
            null_string='',
            add_quotes=True,
            allow_overwrite=True)

        logger.debug("Fetch the list of objects")
        s3_keys = self.__s3.list(s3_path.lstrip('/'))
        local_files = list(map(lambda key: os.path.join(local_path, os.path.basename(key)), s3_keys))

        logger.debug("Create temporary directory: %s", local_path)
        os.mkdir(local_path, 0o700)

        logger.debug("Download all objects")
        for s3_key, local_file in zip(s3_keys, local_files):
            self.__s3.download(key=s3_key, filename=local_file)

        logger.debug("Merge all objects")
        with open(filename, 'wb') as out:
            if columns is not None:
                out.write(gzip.compress((','.join(columns) + os.linesep).encode()))

            for local_file in local_files:
                logger.debug("Merge %s into result file", local_file)

                with open(local_file, 'rb') as read:
                    shutil.copyfileobj(read, out, 2 * MB)

        logger.debug("Remove all objects in S3")
        self.__s3.delete(s3_keys)

        logger.debug("Remove temporary directory in local")
        shutil.rmtree(local_path)

    @staticmethod
    def __generate_session_id() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def __generate_path(prefix: str, session_id: str, suffix: str = '') -> str:
        return ''.join([os.path.join(prefix, session_id), suffix])
