import uuid

from pyspark.sql import SparkSession

from JobArguments import JobArguments
from Storage import Storage
from logger import *


class Job:
    """
    This class represents a Spark Job
    
    Attributes
    ----------
    spark : SparkSession
        The Spark Session object
    storage : Storage
        The Storage connection configuration
    args : JobArguments
        The system arguments
    """
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        self.storage = Storage.get_from_runtime(self.spark)
        self.storage.connect(self.spark.sparkContext)
        self.args = JobArguments.get_from_runtime()
        
        if not self.args:
            LOGGER.setLevel(logging.FATAL)
            LOGGER.fatal("Missing required arguments! Killing Job!")
            
            self.spark.stop()
        else:
            LOGGER.setLevel(logging.DEBUG)
            LOGGER.debug(f"\n-----\n{self.args}\nWith {self.storage}\n-----")
