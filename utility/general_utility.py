import json
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql.types import StructType
with open(r"C:\Users\india\PycharmProjects\Framework_JUNE_rohit1\schema\contact_info_schema.json", 'r') as schema_file:
    schema = StructType.fromJson(json.load(schema_file))
df = spark.read.schema(schema).option("header", True).option("delimiter", ",").csv(r"C:\Users\india\PycharmProjects\Framework_JUNE_rohit1\files\Contact_info_source.csv")
df.printSchema()
