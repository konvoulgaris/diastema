from pyspark.sql import SparkSession
from pyspark.context import SparkContext

from logger import *

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = "9000"
DEFAULT_USER = "minioadmin"
DEFAULT_PASS = "minioadmin"


class Storage:
    """
    This class represents a Storage connection configuration
    
    Attributes
    ----------
    host : str
        The host IP address to connect to
    port : str
        The port to connect to
    username : str
        The username to connect with
    password : str
        The password to connect with
    """
    def __init__(self, host: str, port: str, username: str, password: str):
        self.host     = host
        self.port     = port
        self.username = username
        self.password = password

    
    def __repr__(self) -> str:
        return f"Storage: {self.host}:{self.port}\n  Username: {self.username}\n  Password: {self.password}"

    
    def connect(self, ctx: SparkContext):
        """
        Set the Storage connection configuration for a Spark context

        Parameters
        ----------
        ctx : SparkContext
            The Spark context to use
        """
        ctx._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        ctx._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        ctx._jsc.hadoopConfiguration().set("fs.s3a.endpoint", f"http://{self.host}:{self.port}")
        ctx._jsc.hadoopConfiguration().set("fs.s3a.access.key", self.username)
        ctx._jsc.hadoopConfiguration().set("fs.s3a.secret.key", self.password)
        ctx._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")


    @staticmethod
    def get_from_runtime(session: SparkSession):
        """
        Generates a Storage object from the environment variables in the runtime
        configuration

        Parameters
        ----------
        session : SparkSession
            The Spark Session to get the runtime configuration from

        Returns
        -------
        Storage
            The resulting Storage object
        """
        host     = session.conf.get("spark.executorEnv.MINIO_HOST", DEFAULT_HOST)
        port     = session.conf.get("spark.executorEnv.MINIO_PORT", DEFAULT_PORT)
        username = session.conf.get("spark.executorEnv.MINIO_USER", DEFAULT_USER)
        password = session.conf.get("spark.executorEnv.MINIO_PASS", DEFAULT_PASS)

        LOGGER.setLevel(logging.WARN)

        if host == DEFAULT_HOST:
            LOGGER.warn(f"Using default value for MINIO_HOST: '{DEFAULT_HOST}'")
        if port == DEFAULT_PORT:
            LOGGER.warn(f"Using default value for MINIO_PORT: '{DEFAULT_PORT}'")
        if username == DEFAULT_USER:
            LOGGER.warn(f"Using default value for MINIO_USER: '{DEFAULT_USER}'")
        if password == DEFAULT_PASS:
            LOGGER.warn(f"Using default value for MINIO_PASS: '{DEFAULT_PASS}'")

        return Storage(host, port, username, password)
