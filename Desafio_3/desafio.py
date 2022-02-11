%pyspark

import pandas as pd
import glob


df = pd.read_json("Sellers.json")

cols = df.body[0].keys()



lista_series = []
for i in cols:
    lista_series.append(df.body.apply(lambda x: x[i]))
    
    

pd.DataFrame({
    
    cols[0]:lista_series[0],
    cols[1]:lista_series[1],
    cols[2]:lista_series[2],
    cols[3]:lista_series[3],
    cols[4]:lista_series[4],
    cols[5]:lista_series[5],
    cols[6]:lista_series[6],
    cols[7]:lista_series[7],
    cols[8]:lista_series[8],
    cols[9]:lista_series[9],
    cols[10]:lista_series[10],
    cols[11]:lista_series[11],
    cols[12]:lista_series[12],
    cols[13]:lista_series[13]

}).to_csv("Sellers.csv", sep=';')


Sellers = spark.read.csv("Sellers.csv",sep=';', header=True)
Sellers.createOrReplaceTempView("Sellers")



df = spark.sql("""

select Site_id as siteId, id as sellerId, nickname as sellerNickname, points as sellerPoints  from Sellers

""")
df.createOrReplaceTempView("df")


df_negativos = spark.sql("""

    select * from df where sellerPoints < 0

""")
df_negativos.createOrReplaceTempView("df_negativos")

df_positivos = spark.sql("""

    select * from df where sellerPoints > 0

""")
df_positivos.createOrReplaceTempView("df_positivos")

df_ceros = spark.sql("""

    select * from df where sellerPoints = 0

""")

df_ceros.createOrReplaceTempView("df_ceros")




df_negativos.write.csv("df_negativos" , mode='overwrite', sep=";", header=True)
df_positivos.write.csv("df_positivos" , mode='overwrite', sep=";", header=True)
df_ceros.write.csv("df_ceros" , mode='overwrite', sep=";", header=True)


import os
import boto3

def aws_session(region_name='us-east-2'):
    return boto3.session.Session(aws_access_key_id='AKIAVFDVJEYWHNT6T5FX',
                                aws_secret_access_key='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',
                                region_name=region_name)
                                

session = aws_session()
s3_resource = session.resource('s3')


s3_resource
bucket = s3_resource.Bucket('desafionubimetrics')

files_ceros = [f for f in glob.glob("df_ceros/*.csv")]
files_positivos = [f for f in glob.glob("df_positivos/*.csv")]
files_negativos = [f for f in glob.glob("df_negativos/*.csv")]



for file in files_ceros:


    bucket.upload_file(
        
        Filename=file,
        Key="desafio3/output/MPE/"+str(datetime.datetime.now().year)+"/"+str(datetime.datetime.now().month)+"/"+str(datetime.datetime.now().day)+"/ceros/Sellers.csv",
        ExtraArgs={'ACL': 'public-read'}
    )


for file in files_positivos:


    bucket.upload_file(
        
        Filename=file,
        Key="desafio3/output/MPE/"+str(datetime.datetime.now().year)+"/"+str(datetime.datetime.now().month)+"/"+str(datetime.datetime.now().day)+"/positivos/Sellers.csv",
        ExtraArgs={'ACL': 'public-read'}
    )
    
    
for file in files_negativos:


    bucket.upload_file(
        
        Filename=file,
        Key="desafio3/output/MPE/"+str(datetime.datetime.now().year)+"/"+str(datetime.datetime.now().month)+"/"+str(datetime.datetime.now().day)+"/negativos/Sellers.csv",
        ExtraArgs={'ACL': 'public-read'}
    )
