

def read_file(path,type,spark):
    if type == 'csv':
        df=spark.read.format("csv").option("header",True).option("inferschema",True).load(path)
        return df

