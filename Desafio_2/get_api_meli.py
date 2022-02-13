# pip3 install requests, pandas

import requests
import pandas as pd
from datetime import datetime

primer_offset = 0 
r = requests.get(f'https://api.mercadolibre.com/sites/MLA/search?q=items&offset={primer_offset}')
r.status_code
df = pd.DataFrame(r.json()['results'])


nombre_api='search'
nombre_formato='json'
anio=str(datetime.today().year)
mes=str(datetime.today().month).zfill(2)
try:
    os.mkdir(f"{nombre_api}{nombre_formato}{anio}{mes}")
except:
    print("Ya existe carpeta...")
df.to_json(f'{nombre_api}{nombre_formato}{anio}{mes}/{nombre_api}{nombre_formato}{anio}{mes}.json', orient="records")