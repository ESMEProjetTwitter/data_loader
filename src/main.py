from storage_utils import read_data_from_gcs
from storage_utils import read_data_count_from_gcs
from bigquery_utils import write_tweets_to_bq
from bigquery_utils import write_tweets_count_to_bq

keywords = ["macron", "zemmour", "le pen", "mélenchon"]

for keyword in keywords:
    keyword_data = read_data_from_gcs("2022-05-15", keyword)
    write_tweets_to_bq(keyword_data, keyword)
    keyword_count_data = read_data_count_from_gcs("2022-05-15", keyword)
    write_tweets_count_to_bq(keyword_count_data, keyword)


