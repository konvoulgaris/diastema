import pandas as pd

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

from Job import Job
from logger import *


def main():
    job = Job()

    if not job.args:
        return 1

    if job.args.algorithm.upper() == "KMEANS":
        model = KMeans()
    else:
        LOGGER.setLevel(logging.FATAL)
        LOGGER.fatal(f"Unknown classification algorithm: '{job.args.algorithm}'")
        return 1
    
    LOGGER.setLevel(logging.DEBUG)
    
    # Load data
    LOGGER.debug("Loading data...")
    raw = job.spark.read.option("header", True).option("inferSchema", True).csv(f"s3a://{job.args.input_path}/*")
    LOGGER.debug("Loaded data")

    # Assemble data
    LOGGER.debug("Assembling data...")
    # features = raw.drop(job.args.target_column).columns
    assembler = VectorAssembler(inputCols=raw.columns, outputCol="features")
    data = assembler.transform(raw)
    # train, test = data.randomSplit([0.8, 0.2])
    LOGGER.debug("Assembled data")

    # Fit and transform
    LOGGER.debug("Fitting...")
    model = model.fit(data)
    # pred = model.transform(data)
    LOGGER.debug("Fitted")

    # Export results
    LOGGER.debug("Exporting results...")
    out = model.clusterCenters()[0]
    out = pd.DataFrame(out, columns=["clusterCenters"])
    out = job.spark.createDataFrame(out)
    out.repartition(1).write.csv(path=f"s3a://{job.args.output_path}", header="true", mode="overwrite")
    LOGGER.debug("Exported results")

    return 0

if __name__ == "__main__":
    code = main()
    exit(code)
