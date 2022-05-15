from storage_utils import read_data_from_gcs
from bigquery_utils import write_tweets_to_bq

keywords = ["macron"]

for keyword in keywords:
    keyword_data = read_data_from_gcs("2022-05-08", keyword)
    write_tweets_to_bq(keyword_data, keyword)



