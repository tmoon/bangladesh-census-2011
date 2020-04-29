import camelot
import datetime
from joblib import Parallel, delayed
import os
from PyPDF2 import PdfFileReader
import pandas as pd
import re

from consts import *

TABLE_OFFICIAL_NUM_COLS = {
    1: 8,
    2: 10,
    3: 12,
    4: 13,
    5: 12, 
    6: 11, 
    7: 14,
    8: 14, 
    9: 10,
    10: 13,
    11: 11,
    12: 10,
    13: 8,
    14: 11,
    15: 10

}

DEBUG = 0


class CensusPdfParser(object):
    """docstring for CensusPdfParser"""
    def __init__(self, district, filepath=None, table_id=1):
        super(CensusPdfParser, self).__init__()
        self.district = district
        self.table_id = table_id
        self.filename = "%s_C%.2d" % (self.district, self.table_id)
        
        self.filepath = PDF_SAVE_DIR + "/%s.pdf" % self.filename if filepath is None else filepath

        self.num_pages = 0
        self.table_idx_arr = []

    def find_num_pages(self):
        file = open(self.filepath, 'rb')

        # creating a pdf reader object
        fileReader = PdfFileReader(file)
        self.num_pages = fileReader.getNumPages()
        if DEBUG:
            print(self.num_pages, "Pages")

    def find_last_header_row(self, tmp_table):
        # find the last header row
        last_row_id = -1
        for id, row in tmp_table.df.iterrows():
            numeric_rows = re.findall(r'\d', "".join(row))
            # hardcoded check 1-6 in row
            if "".join([str(i) for i in range(1, 7)]) in "".join(numeric_rows):
                # print("last header row", id)
                last_row_id = id
                break
        if last_row_id == -1:
            print("FAILED to find last header col!!!", self.filename)
            assert(last_row_id != -1)
        # if last_row_id == -1:
        #     print(tmp_table.page)
        #     print(tmp_table.df)
        #     for id, row in tmp_table.df.iterrows():
        #         numeric_rows = re.findall(r'\d', "".join(row))
        #         print(numeric_rows)


        return last_row_id

    def find_header_boundary(self):
        # tmp_table = tmp_table = camelot.read_pdf(self.filepath, flavor='stream', pages=1)
        # self.find_last_header_row = self.find_last_header_row(tmp_table)
        pass
        

    def find_column_boundary(self):
        # read the header table
        tmp_table = camelot.read_pdf(self.filepath, flavor='lattice', pages='1', line_scale=100, iteration=1)

        cell_arr = []
        for c in tmp_table[0].cells[0][:-1]:
            cell_arr.append(c.x2)

        # return cell boundaries
        self.column_boundary = ",".join(map(str, cell_arr))
        if DEBUG:
            print(self.column_boundary)

    def read_tables(self):
        self.tables = None
        # if DEBUG:
        #     self.num_pages = 2
        try:
            self.tables = camelot.read_pdf(self.filepath, flavor='stream', pages='1-%d' % self.num_pages,
                         columns=[self.column_boundary for i in range(self.num_pages)])
        except Exception as e:
            tmp_table = camelot.read_pdf(self.filepath, flavor='stream', pages='1-%d' % self.num_pages)
            total_tables = len(tmp_table)

            self.tables = camelot.read_pdf(self.filepath, flavor='stream', pages='1-%d' % self.num_pages,
                         columns=[self.column_boundary for i in range(total_tables)])
        
        last_header_row = self.find_last_header_row(self.tables[0])

        if DEBUG:
            print(self.tables[0].df.iloc[last_header_row + 1:])

    def find_pages_to_parse(self):
        if len(self.tables) == self.num_pages:
            self.table_idx_arr = range(self.num_pages)
        else:
            td = {}
            table_idx_arr = []

            for id, t in enumerate(self.tables):
                if td.get(t.page, None) is None:
                    td[t.page] = 1
                    table_idx_arr.append(id)
                else:
                    td[t.page] += 1
            self.table_idx_arr = table_idx_arr

    def parse_all_tables(self):
        df_arr = []
        for id in self.table_idx_arr:
            last_header_row = self.find_last_header_row(self.tables[id])
            df_arr.append(self.tables[id].df.iloc[last_header_row + 1:])

        self.parsed_df = pd.concat(df_arr).reset_index(drop=1)

    def format_and_dump_tables(self, out_filepath=None):
        row_arr = []
        for _, row in self.parsed_df.iterrows():
            if row[6] == '' and 'Total' in row[5]:
                row[6] = row[5]
                row[5] = ''
            row_arr.append(row)

        self.parsed_df = pd.DataFrame(row_arr)

        print("DONE! ", self.district, self.table_id, self.num_pages, self.parsed_df.shape, "\n", self.parsed_df.head())

        out_filepath = CSV_SAVE_DIR + "/%s.csv" % self.filename if out_filepath is None else out_filepath
        self.parsed_df.to_csv(out_filepath, index=0)

    def run(self):
        self.find_num_pages()
        self.find_header_boundary()
        self.find_column_boundary()
        self.read_tables()
        self.find_pages_to_parse()
        self.parse_all_tables()
        self.format_and_dump_tables()

def run_parallel(args):
    district, table_id = args
    try:
        parser = CensusPdfParser(district, table_id=table_id)
        parser.run()
    except Exception as e:
        with open(META_SAVE_DIR +  "/%s_C%.2d.error" % (district, table_id), 'w') as f:
            f.write("%s FAILED: %s" % (str(datetime.datetime.now()), e))
        print("FAILED", args, e)
        raise e
    

if __name__ == '__main__':
    # run_parallel(["Bandarban", 1])
    # parser = CensusPdfParser('Bhola', table_id=1)
    # parser.run()
    df = pd.read_csv(META_SAVE_DIR + "/census_urls_cleaned.csv")
    arg_array = []
    for d in set(df.district.values):
        for i in range(1, 16):
            arg_array.append([d, i])

    parallel_worker = Parallel(n_jobs=MAX_CPUS, backend='multiprocessing', verbose=50)
    parallel_worker(delayed(run_parallel)(arg) for arg in arg_array)

        