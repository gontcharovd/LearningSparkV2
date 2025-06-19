from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.appName("PythonMnMCount").getOrCreate()

mnm_file = '/home/denis/code/LearningSparkV2/chapter2/py/src/mnm_dataset.csv'

mnm_df = (
    spark
    .read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(mnm_file)
)

count_df = (mnm_df
 .groupby('State', 'Color')
 .agg(F.sum('Count'))
)

count_df.show()

spark.stop()

