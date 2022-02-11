%pyspark

df = spark.read.csv("MPE1004/*.csv", header=True, sep=';')
df.createOrReplaceTempView("df")

visits = spark.read.csv("visits.csv", header=True)
visits.createOrReplaceTempView("visits")


join_df = spark.sql("""

    SELECT df.itemId, df.soldQuantity, visits.visits
      FROM df
      JOIN visits
        ON df.itemId = visits.itemId
     WHERE df.soldQuantity>0

""")
join_df.createOrReplaceTempView("join_df")

join_df.write.csv("visits_join" , mode='overwrite', sep=";", header=True)

files = [f for f in glob.glob("visits_join/*.csv")]

for file in files:

    bucket.upload_file(
        
        Filename=file,
        Key="desafio5/output/"+str(datetime.datetime.now().year)+"/"+str(datetime.datetime.now().month)+"/"+str(datetime.datetime.now().day)+"/"+file,
        ExtraArgs={'ACL': 'public-read'}
    )
    