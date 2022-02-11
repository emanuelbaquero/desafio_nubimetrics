%pyspark


import glob
import json
from pandas.io.json import json_normalize

with open('MPE1004.json') as json_data:
    data = json.load(json_data)
    
df = pd.DataFrame(data['results'])
df = df.reset_index()

df = spark.createDataFrame(df)
df.createOrReplaceTempView("df")

df = spark.sql("""

    select index+1 as rowId, id as itemId, sold_quantity as soldQuantity, available_quantity as availableQuantity from df

""")

df.createOrReplaceTempView("df")

df.write.csv("MPE1004" , mode='overwrite', sep=";", header=True)

files = [f for f in glob.glob("MPE1004/*.csv")]

for file in files:


    bucket.upload_file(
        
        Filename=file,
        Key="desafio4/output/"+str(datetime.datetime.now().year)+"/"+str(datetime.datetime.now().month)+"/"+str(datetime.datetime.now().day)+"/"+file,
        ExtraArgs={'ACL': 'public-read'}
    )
    
