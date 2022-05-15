from google.cloud import bigquery
import datetime
import pandas
#import pytz

TWEETS_TABLE_ID = "esme-twitter-projet.tweets.tweets"
TWEETS_COUNT_TABLE_ID = "esme-twitter-projet.tweets.tweets_count"

def write_tweets_to_bq(tweets_data, keyword):

    # Construct a BigQuery client object.
    client = bigquery.Client()

    tweets_df = pandas.DataFrame(
        tweets_data,
        # In the loaded table, the column order reflects the order of the
        # columns in the DataFrame.
        columns=[
            "id",
            "text",
        ],
    )
    tweets_df["keyword"] = keyword

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING, mode="REQUIRED"),
            bigquery.SchemaField("text", bigquery.enums.SqlTypeNames.STRING, mode="REQUIRED"),
            bigquery.SchemaField("keyword", bigquery.enums.SqlTypeNames.STRING, mode="REQUIRED"),
        ],
    )

    job = client.load_table_from_dataframe(
        tweets_df, TWEETS_TABLE_ID, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(TWEETS_TABLE_ID)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), TWEETS_TABLE_ID
        )
    )

def write_tweets_count_to_bq(tweets_data, keyword):

    # Construct a BigQuery client object.
    client = bigquery.Client()

    tweets_counts_df = pandas.DataFrame(
        tweets_data,
        # In the loaded table, the column order reflects the order of the
        # columns in the DataFrame.
        columns=[
            "start",
            "end",
            "tweet_count"
        ],
    )
    tweets_counts_df["keyword"] = keyword

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("start", bigquery.enums.SqlTypeNames.STRING, mode="REQUIRED"),
            bigquery.SchemaField("end", bigquery.enums.SqlTypeNames.STRING, mode="REQUIRED"),
            bigquery.SchemaField("tweet_count", bigquery.enums.SqlTypeNames.INTEGER, mode="REQUIRED"),
            bigquery.SchemaField("keyword", bigquery.enums.SqlTypeNames.STRING, mode="REQUIRED"),
        ],
    )

    job = client.load_table_from_dataframe(
        tweets_counts_df, TWEETS_COUNT_TABLE_ID, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(TWEETS_COUNT_TABLE_ID)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), TWEETS_COUNT_TABLE_ID
        )
    )





