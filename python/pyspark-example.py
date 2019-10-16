import pyspark

df = sqlContext.createDataFrame([('a', 1, 'null'), ('b', 1, 1), ('c', 1, 'null'), ('d', 'null', 1), ('e', 1, 1)],  # ,('f',1,'NaN'),('g','bla',1)],
                                schema=('id', 'foo', 'bar')
                                )

foobar_df = df.filter((df.foo == 1) & (df.bar.isNull()))


