from google.cloud import bigquery
from google.cloud.exceptions import NotFound

TWEETS_TABLE_ID = "esme-twitter-projet.tweets.tweets"
TWEETS_COUNT_TABLE_ID = "esme-twitter-projet.tweets.tweets_count"

client = bigquery.Client()


def create_tweet_table():
    schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("text", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("keyword", "STRING", mode="REQUIRED"),
    ]

    table = bigquery.Table(TWEETS_TABLE_ID, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


def create_tweet_count_table():
    schema = [
        bigquery.SchemaField("start", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("end", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tweet_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("keyword", "STRING", mode="REQUIRED"),
    ]

    table = bigquery.Table(TWEETS_COUNT_TABLE_ID, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

try:
    client.get_table(TWEETS_TABLE_ID)  # Make an API request.
    print("Table {} already exists.".format(TWEETS_TABLE_ID))
except NotFound:
    print("Table {} is not found.".format(TWEETS_TABLE_ID))
    create_tweet_table()


try:
    client.get_table(TWEETS_COUNT_TABLE_ID)  # Make an API request.
    print("Table {} already exists.".format(TWEETS_COUNT_TABLE_ID))
except NotFound:
    print("Table {} is not found.".format(TWEETS_COUNT_TABLE_ID))
    create_tweet_count_table()