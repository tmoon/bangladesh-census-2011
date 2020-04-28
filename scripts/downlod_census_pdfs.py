from joblib import Parallel, delayed
import pandas as pd
import time
import os.path
from os import path
import requests
from consts import *


def get_urls():
    url_df = pd.read_csv(META_SAVE_DIR + "/census_urls.csv")
    return url_df.url.values


def download_file(final_url):
    cache = True
    
    print("Downloading from %s" % final_url)
    t = time.time()
    path_name = PDF_SAVE_DIR + "/" + final_url.split("/")[-1]
    if path.exists(path_name) and cache:
        print("FROM CACHE " + path_name)
    else:
        pdfFile = requests.get(final_url, allow_redirects=True)
        with open(path_name, 'wb') as f:
            f.write(pdfFile.content)
        
    print("Time taken:", time.time() - t)


if __name__ == '__main__':
    # download_file('http://203.112.218.65:8008/Census2011/Barisal/Barisal/Barisal_C05.pdf')

    urls = get_urls()
    # print(urls
    parallel_worker = Parallel(n_jobs=MAX_THREADS, backend='threading', verbose=50)
    parallel_worker(delayed(download_file)(url) for url in urls)