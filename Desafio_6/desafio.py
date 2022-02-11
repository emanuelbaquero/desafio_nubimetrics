%pyspark

df = spark.read.csv("MPE1004/*.csv", header=True, sep=';')
df.createOrReplaceTempView("df")

visits = spark.read.csv("visits.csv", header=True)
visits.createOrReplaceTempView("visits")


join_df = spark.sql("""

    SELECT df.itemId, df.soldQuantity, visits.visits, df.soldQuantity/visits.visits as conversionRate
      FROM df
      JOIN visits
        ON df.itemId = visits.itemId
     WHERE df.soldQuantity>0
  ORDER BY df.soldQuantity/visits.visits DESC

""")
join_df.createOrReplaceTempView("join_df")

join_df = spark.createDataFrame(join_df.toPandas().reset_index())
join_df.createOrReplaceTempView("join_df")

join_df = spark.sql("""

    SELECT itemId, soldQuantity, visits, conversionRate, index+1 as conversionRanking
      FROM join_df

""")


join_df.repartition(1).write.csv("df_ranking" , mode='overwrite', sep=";", header=True)

files = [f for f in glob.glob("df_ranking/*.csv")]

for file in files:

    bucket.upload_file(
        
        Filename=file,
        Key="desafio6/output/"+str(datetime.datetime.now().year)+"/"+str(datetime.datetime.now().month)+"/"+str(datetime.datetime.now().day)+"/"+file,
        ExtraArgs={'ACL': 'public-read'}
    )

