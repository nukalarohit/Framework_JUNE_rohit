from pyspark.sql import SparkSession
spark=SparkSession.builder.getOrCreate()

df=spark.read.format("csv").option("header",True).option("inferschema",True).load(r"C:\Users\india\PycharmProjects\Framework_JUNE_rohit1\files\Contact_info_source.csv")
# df.show()

dfjson=df.schema.json()
print(dfjson)