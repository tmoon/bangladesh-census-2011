from bs4 import BeautifulSoup
import requests
import re
from consts import *
import pandas as pd

urls = [BASE_CENSUS_WEB_URL + "/PageWebMenuContent.aspx?MenuKey=332", # barisal ctg
        BASE_CENSUS_WEB_URL + "/PageWebMenuContent.aspx?MenuKey=333", # dhk khulna
        BASE_CENSUS_WEB_URL + "/PageWebMenuContent.aspx?MenuKey=344", # raj rangpur
        BASE_CENSUS_WEB_URL + "/PageWebMenuContent.aspx?MenuKey=345"] # sylhet

row_arr = []
for url in urls:
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    for link in soup.select('#mid-area')[0].findAll('a', attrs={'href': re.compile("_C.+.pdf")}):
        dt = link.get('href')
        meta = dt.split("/")[-4:]
        meta.append(BASE_CENSUS_WEB_URL + "" + dt)
        print(meta)
        row_arr.append(meta)

df = pd.DataFrame(row_arr, columns=["census_id", "division", "district", "file_name",
    "url"]).to_csv(META_SAVE_DIR + "/census_urls.csv", index=0)

# NOTE: once downloaded, need to fix Barisal_Zila abd Gaibandha