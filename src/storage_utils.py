from google.cloud import storage
import json

client = storage.Client()

def read_data_from_gcs(date, tweet_keyword):
    bucket = client.get_bucket('tweets-project-esme')
    blob = bucket.get_blob("tweets/{}/{}/tweets.json".format(date, tweet_keyword))
    data = json.loads(blob.download_as_string(client=None))
    return data

def read_data_count_from_gcs(date, tweet_keyword):
    bucket = client.get_bucket('tweets-project-esme')
    blob = bucket.get_blob("tweets/{}/{}/tweets_count.json".format(date, tweet_keyword))
    data = json.loads(blob.download_as_string(client=None))
    data_to_list = data["data"]
    return data_to_list

