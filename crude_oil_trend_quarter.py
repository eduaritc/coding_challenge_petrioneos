from bs4 import BeautifulSoup as bSoup
import requests
from dateutil.parser import parse
import pandas as pd
from datetime import datetime
from dateutil.parser import parse
from os.path import exists
import logging
import schedule
import time

URL = "https://www.gov.uk/government/statistics/oil-and-oil-products-section-3-energy-trends"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
}

DOWNLOADS = "downloads/"
DOM = requests.get(URL, headers=HEADERS)
SUPLY_SOUP = bSoup(DOM.content, "html.parser", from_encoding="UTF-8")
A_EXCEL_FILE  = SUPLY_SOUP.find("a", {"aria-describedby": "attachment-7159263-accessibility-help"})
FILE_URL = A_EXCEL_FILE.get("href")
FILENAME_FULL_PATH = DOWNLOADS+FILE_URL.split("/")[-1]

logger = logging.getLogger("petrioneos")
logger.setLevel(logging.INFO)
handler = logging.FileHandler('logs_petrioneos.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


# logger.warning('This is a warning message')
# logger.error('This is an error message')


def check_new_data(df_crude_oil):
      logger.info('Looking for new data...')
      print("Looking for new data...")
      download_file()
      df_recent_crude_oil = pd.read_excel(FILENAME_FULL_PATH, \
                              sheet_name = "Quarter", \
                              index_col = 4, \
                              header = 4)
      if df_crude_oil.equals(df_recent_crude_oil):
            logger.info('The two dataframes are equal')
            print('The two dataframes are equal')
            return False
      else:
            logger.info('The two dataframes are not equal')
            print('The two dataframes are not equal')
            return True


def download_file():
      resp = requests.get(FILE_URL)
      crude_oil_file  = open(FILENAME_FULL_PATH, "wb")
      crude_oil_file.write(resp.content)
      crude_oil_file.close()

def is_date(string, fuzzy=False):
    try: 
        parse(string, fuzzy=fuzzy)
        return True
    except ValueError:
        logger.error('Error al intentar parsear {} to datetime'.format(string))
        return False



"""
MIMIMUM REQUIRED PROFILING CHECKS
"""
print("--------------------------------------------------")
# Run the task every 24 hours
schedule.every(24).hours.do(check_new_data)
download_file()
df_crude_oil = pd.read_excel(FILENAME_FULL_PATH, \
                              sheet_name = "Quarter", \
                              index_col = 4, \
                              header = 4)

num_rows, num_cols = df_crude_oil.shape
min_per_col = df_crude_oil.min()
max_per_col = df_crude_oil.max()
mean_per_col = df_crude_oil.mean()
median_per_col = df_crude_oil.median()
total_missing_values = df_crude_oil.isnull().sum().sum()
dict_profilling = {"num_rows":num_rows,
                   "num_cols": num_cols,
                   "min_per_col": min_per_col,
                   "max_per_col":max_per_col,
                   "mean_per_col":mean_per_col,
                   "median_per_col":median_per_col,
                   "total_missing_values":total_missing_values}
df_crude_oil.fillna("")

df_profiling = pd.DataFrame(dict_profilling)

df_profiling.to_csv(FILENAME_FULL_PATH.split("/")[-1][0:-5]+"_data_profiling.csv")

#Checking for dates inside of the dataframe and if so, formatting them
if (df_crude_oil.select_dtypes(include=['datetime']).columns.size > 0):
    logger.info('DataFrame contains date type column')
    print('DataFrame contains date type column')
    for i, j in df_crude_oil.iterrows():
      for data in j:
            str_data = str(data)
            if is_date(str_data):
                  if len(str_data) == 10:
                        df_crude_oil.at[i][j] = datetime.strptime(str_data, "%Y-%m-%d")
                  elif len(str_data) == 17:
                        df_crude_oil.at[i][j] = datetime.strptime(str_data, '%Y-%m-%d %H-%M-%S')
else:
      logger.info('DataFrame does not contain date type column')
      print('DataFrame does not contain date type column')
dict_data_consistency = {}
dict_data_consistency["new_data"] = check_new_data(df_crude_oil)
dict_data_consistency["correct_time_format"] = "sorted"
dict_data_consistency["num_missing_values"] = total_missing_values
dict_data_consistency["new_columns"] = check_new_data(df_crude_oil)
dict_data_consistency["missing_columns"] = check_new_data(df_crude_oil)
df_data_consistency = pd.DataFrame(dict_data_consistency.items())
df_data_consistency.to_csv(FILENAME_FULL_PATH.split("/")[-1][0:-5]+"_data_consistency.csv")
print("--------------------------------------------------")

