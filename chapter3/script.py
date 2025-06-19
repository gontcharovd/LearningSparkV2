from pyspark.sql.types import (
    ArrayType,
    DateType,
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType
)
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("my_script").getOrCreate()
    schema = StructType([
        StructField("Id", IntegerType(), False),
        StructField("First", StringType(), False),
        StructField("Last", StringType(), False),
        StructField("Url", StringType(), False),
        StructField("Published", DateType(), False),
        StructField("Hits", IntegerType(), False),
        StructField("Campaigns", ArrayType(StringType()), False),
    ])
    df = spark.read.json("data/blogs.json", schema=schema)
    df.show()
    spark.stop()
