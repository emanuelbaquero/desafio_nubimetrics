%pyspark

df = spark.read.csv("MPE1004/*.csv", header=True, sep=';')
df.createOrReplaceTempView("df")

sum_availableQuantity = int(df.toPandas().availableQuantity.astype(int).sum())

df = spark.sql("""

    select itemId, availableQuantity, availableQuantity/(""" + str(sum_availableQuantity) + """*0.01) as stockPercentage
    from df
    order by stockPercentage desc
""")

df.createOrReplaceTempView("df")

df.repartition(1).write.csv("df_porc_stock" , mode='overwrite', sep=";", header=True)

files = [f for f in glob.glob("df_porc_stock/*.csv")]

for file in files:

    bucket.upload_file(
        
        Filename=file,
        Key="desafio7/output/"+str(datetime.datetime.now().year)+"/"+str(datetime.datetime.now().month)+"/"+str(datetime.datetime.now().day)+"/"+file,
        ExtraArgs={'ACL': 'public-read'}
    )

