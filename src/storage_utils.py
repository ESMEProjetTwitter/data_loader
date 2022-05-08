from google.cloud import storage

client = storage.Client()

def read_data_from_gcs(date, tweet_keyword):
    bucket = client.get_bucket('tweets-project-esme')
    blob = bucket.get_blob("/tweets/2022-05-08/macron/tweets.json")
    print(blob)

