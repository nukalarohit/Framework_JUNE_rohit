from utility.reporting import write_output


def count_check(source, target, spark, row, validation, out):
    print("*************** count_check ********************")
    source.createOrReplaceTempView('sqlsource')
    target.createOrReplaceTempView('sqltarget')
    source_cnt = spark.sql("select * from sqlsource").count()
    target_cnt = spark.sql("select * from sqltarget").count()
    print(source_cnt, type(source_cnt))
    print(target_cnt, type(target_cnt))
    failed = source_cnt - target_cnt
    if source_cnt == target_cnt:
        print(f"{source_cnt} source count is matching with {target_cnt} target count ")
        write_output(validation_type=validation, source_name=row['source'], target_name=row['target']
                     , no_of_source_record_count=source_cnt, no_of_target_record_count=target_cnt
                     , failed_count=failed, column=row['key_col_list'], status='PASS', source_type=row['source_type'],
                     target_type=row['target_type'], out=out)
    else:
        print(f"{source_cnt} source count is NOT matching with {target_cnt} target count ")
        write_output(validation_type=validation, source_name=row['source'], target_name=row['target']
                     , no_of_source_record_count=source_cnt, no_of_target_record_count=target_cnt
                     , failed_count=failed, column=row['key_col_list'], status='FAIL', source_type=row['source_type'],
                     target_type=row['target_type'], out=out)


def null_value_check(source,target, spark, row, validation, out, nullcols):
    print("*************** null_value_check ********************")
    source_cnt=source.count()
    target_cnt=target.count()
    target.createOrReplaceTempView('sqlnulltarget')
    nullcols = nullcols.split(',')
    for i in nullcols:
        failed = spark.sql(f"select * from sqlnulltarget where {i} is null").count()
        if failed > 0:
            print(f"{failed} NULL values present in {i} ")
            write_output(validation_type=validation, source_name=row['source'], target_name=row['target']
                         , no_of_source_record_count=source_cnt, no_of_target_record_count=target_cnt
                         , failed_count=failed, column=i, status='PASS',
                         source_type=row['source_type'],
                         target_type=row['target_type'], out=out)
        else:
            print("NO NULL values present")
            write_output(validation_type=validation, source_name=row['source'], target_name=row['target']
                         , no_of_source_record_count=source_cnt, no_of_target_record_count=target_cnt
                         , failed_count=failed, column=i, status='FAIL',
                         source_type=row['source_type'],
                         target_type=row['target_type'], out=out)


def duplicate(source, target, spark, dupcols):
    print("*************** duplicate check ********************")
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
    print("*************** data_compare ********************")
    source.createOrReplaceTempView("sourcesql")
    target.createOrReplaceTempView("targetsql")
    source_target_compare = spark.sql(f"select * from sourcesql minus select * from targetsql")
    # Source Minus Target
    source_target_compare_cnt = source_target_compare.count()
    print(source_target_compare_cnt)
    if source_target_compare_cnt > 0:
        print("source data and target does not match and the records present in Source but not in Target are : ")
        source_target_compare.show()
    else:
        print("source data is MATCHING with  target")

    # Target  Minus Source
    target_source_compare = spark.sql(f"select * from targetsql  minus select * from sourcesql")
    target_source_compare_cnt = target_source_compare.count()
    print(target_source_compare_cnt)
    if target_source_compare_cnt > 0:
        print("Target data and Source does not match and the records present in Target  but not in Source are : ")
        target_source_compare.show()
    else:
        print(" Target data is MATCHING with  source")


def uniqueness_check(source, target, spark, uniqueness_cols):
    print("*************** uniqueness_check ********************")
    lstcols = uniqueness_cols.split(',')
    target.createOrReplaceTempView("sqltarget")
    for cols in lstcols:
        uniquecount = spark.sql(f"select {cols},count(*) from sqltarget group by {cols} having count(*)>1")
        uniquecount.show()
        uniquefailed = uniquecount.count()
        if uniquefailed > 0:
            print(f"no of duplicates in column {cols} are :  {uniquefailed}")
        else:
            print(f"There are NO duplicates PRESENT in column {cols} ")
        print()


def schema_check(source, target, spark):
    source.createOrReplaceTempView("sourcevw")
    target.createOrReplaceTempView("targetvw")
    sourcevwdf = spark.sql("describe sourcevw")
    targetvwdf = spark.sql("describe targetvw")
    sourcevwdf.show()
    targetvwdf.show()
    sourcevwdf.createOrReplaceTempView("S")
    targetvwdf.createOrReplaceTempView("T")
    failed_col_names = spark.sql(f"SELECT col_name FROM S MINUS SELECT col_name FROM T")
    print("Below Column Names Mismatched :")
    failed_col_names.show()
    failed_data_types = spark.sql(
        f"SELECT row_number() over(order by col_name) as seq_no ,data_type FROM S MINUS SELECT row_number() over(order by col_name)  as seq_no ,data_type FROM T")
    print("Below Data Type Mismatch :")
    row_sequence = spark.sql("SELECT row_number() over(order by col_name) as seq_no ,col_name,data_type FROM T")
    failed_data_types.show()
    row_sequence.show()


def name_check(target, spark, column):
    lstcols = column.split(',')
    print("*******  NAME CHECK Started  *********")
    print(column)
    target.createOrReplaceTempView("targetvw")
    for i in lstcols:
        targetdf = spark.sql(f"""select Ident_ifier,{i} from targetvw where where regexp_like({i},'[^a-zA-Z]') """)
        targetdf.show()


def column_range_check(target, spark, column, min_val, max_val):
    target.createOrReplaceTempView('tgt')
    tgtdf = spark.sql(f"select {column} from tgt where {column} not between {min_val} and {max_val}")
    print(f"below  {column} values  are not in the range ")
    tgtdf.show()
    tgtdfcnt = tgtdf.count()
    if tgtdfcnt > 0:
        print(f"{column} value are not in range of : {min_val} and {max_val}")
    else:
        print(f"{column} value are WITHIN the range of : {min_val} and {max_val}")

