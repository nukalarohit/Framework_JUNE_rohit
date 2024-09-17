def count_check(source, target, spark):
    source.createOrReplaceTempView('sqlsource')
    target.createOrReplaceTempView('sqltarget')
    source_cnt = spark.sql("select * from sqlsource").count()
    target_cnt = spark.sql("select * from sqltarget").count()
    print(source_cnt, type(source_cnt))
    print(target_cnt, type(target_cnt))
    if source_cnt == target_cnt:
        print(f"{source_cnt} source count is matching with {target_cnt} target count ")
    else:
        print(f"{source_cnt} source count is NOT matching with {target_cnt} target count ")


def null_value_check(source, target, spark, nullcols):
    target.createOrReplaceTempView('sqlnulltarget')
    nullcols = nullcols.split(',')
    for i in nullcols:
        target_null_cnt = spark.sql(f"select * from sqlnulltarget where {i} is null").count()
        if target_null_cnt > 0:
            print(f"{target_null_cnt} NULL values present in {i} ")
        else:
            print("NO NULL values present")


def duplicate(source, target, spark, dupcols):
    target.createOrReplaceTempView('sqlnulltarget')
    # dupcols=dupcols.split(',')
    # for i in dupcols:
    sqlnulltargetcnt = spark.sql(
        f"select {dupcols},count(*) from sqlnulltarget group by {dupcols} having count(*)>1").count()
    if sqlnulltargetcnt > 0:
        print(f"duplicates exists and count is  ", sqlnulltargetcnt)
    else:
        print(f"NO duplicates exists :   ", sqlnulltargetcnt)


def data_compare(source, target, spark):
    source.createOrReplaceTempView("sourcesql")
    target.createOrReplaceTempView("targetsql")
    source_target_compare = spark.sql(f"select * from sourcesql minus select * from targetsql")
    source_target_compare.show()
    source_target_compare_cnt = source_target_compare.count()
    print(source_target_compare_cnt)
    if source_target_compare_cnt > 0:
        print("source data and target does now match")
    else:
        print("source data is MATCHING with  target")
