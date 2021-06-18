

df = spark.read.csv(interim+'XXXXX',header=True,sep=",")
df = df.withColumn('Score',col('Score').cast(FloatType()))
df = df.fillna(0,subset=['Score'])
df1 = df.filter("Score <> 0 ")
 
discretizer = QuantileDiscretizer(numBuckets=15, inputCol='DI_Score', outputCol="quantile_bucket")
result = discretizer.fit(df1).transform(df1)
r1 = result.groupBy('quantile_bucket').agg(countDistinct('a', 'b','c').alias("record_cnt"),min('Score').alias('min_score'),max('Score').alias('max_score'))

r1.show(40,False)



