import json
import requests
import pymongo
import boto3
import os
import datetime

# Setting environmental variables to be used in lambda
DATABASE_NAME = os.getenv("DATABASE_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
MONGODB_URI = os.getenv("MONGODB_URI")
BUCKET_NAME = os.getenv("BUCKET_NAME")

DATA_SOURCE_URL = f"https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/" \
                  f"?field=all&format=json" \
                  f"&date_received_max=<to_date>&date_received_min=<from_date>"

client = pymongo.mongo_client.MongoClient(MONGODB_URI)


def get_from_date_to_date():
    from_date = "2023-06-01"
    from_date = datetime.datetime.strptime(from_date, "%Y-%m-%d")

    if COLLECTION_NAME in client[DATABASE_NAME].list_collection_names():
        # To query the document in descending order to extract the document with latest to_date
        latest_document = client[DATABASE_NAME][COLLECTION_NAME].find_one(sort=[
                                                                          ("to_date", -1)])

        if latest_document is not None:
            from_date = latest_document["to_date"]

    # Assigning the date 2 days before the current date to TO_DATE,
    # because the dats is updated on one day interval, but with the data content 2 dasys prior to the current date.

    current_date = datetime.datetime.now()
    to_date = current_date - datetime.timedelta(days=2)

    response = {
        "from_Date": from_date.strftime("%Y-%m-%d"),
        "to_date": to_date.strftime("%Y-%m-%d"),
        "from_date_object": from_date,
        "to_date_object": to_date
    }
    return response


def ingest_from_date_to_date(data, status=True):
    data.update({"status": status})
    client[DATABASE_NAME][COLLECTION_NAME].insert_one(data)


def lambda_handler(event, context):
    print(event, context)
    from_date, to_date, from_date_object, to_date_object = get_from_date_to_date().values()
    if to_date == from_date:
        return {
            'statusCode': 200,
            'body': json.dumps(f"Pipeline has already downloaded all data upto {from_date}")
        }

    url = DATA_SOURCE_URL.replace(
        "<to_date>", to_date).replace("<from_date>", from_date)
    headers = {'User-agent': 'consumer complaints extractor'}
    response = requests.get(url, headers)
    data = json.loads(response.content)

    consumer_complaints_data = list(map(lambda x: x["_source"],
                                    filter(lambda x: "_source" in x.keys(),
                                           data))
                                    )

    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=f"inbox/{from_date.replace('-', '_')}_{to_date.replace('-', '_')}_consumer_complaints.json",
        Body=bytes(json.dumps(consumer_complaints_data).encode('utf-8'))
    )

    ingest_from_date_to_date(
        {"from_date": from_date_object, "to_date": to_date_object})
    return {
        'statusCode': 200,
        'body': json.dumps("New date Entries have been succefully writted to your mongoDB collection")
    }