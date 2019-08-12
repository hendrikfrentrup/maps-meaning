from functools import lru_cache

@lru_cache(maxsize=None)
def get_spark():
    try:
        sc and spark
    except NameError as e:
        import pyspark
        import pyspark.sql

        sp_context = pyspark.SparkContext()
        sp_session = pyspark.sql.SparkSession.builder\
                        .master("local")\
                        .appName("test")\
                        .getOrCreate()
    return sp_context, sp_session
