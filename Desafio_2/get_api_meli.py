# pip3 install requests, pandas

import requests
import pandas as pd


primer_offset = 0 
r = requests.get(f'https://api.mercadolibre.com/sites/MLA/search?q=items&offset={primer_offset}')
r.status_code
df = pd.DataFrame(r.json()['results'])