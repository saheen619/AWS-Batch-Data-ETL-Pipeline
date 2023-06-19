# AWS Batch Data ETL Pipeline

## Objective:   
This is a batch data ETL pipeline project aimed at extracting data out of a financial consumer complaints API using AWS Services, MongoDB and loading the data into dynamo DB to be used for further processing and applications. To see the overview or the workflow of the pipeline, watch the video [Video - Project Overview](Project%20Overview%20-%20AWS%20Batch%20Data%20ETL%20Pipeline.mp4)

## Architecture Diagram
![Architecture diagram](https://github.com/saheen619/AWS-Batch-Data-ETL-Pipeline/blob/main/architecture-diagram.jpg?raw=true)

## Workflow:

* The Eventbridge trigger scheduled on a one-day interval triggers the Lambda function, which initiates the pipeline.
* Calling the Consumer Complaints API and extracting Data using AWS Lambda.
* Dump the extracted data in json format into the S3 Bucket
* Lambda function returns the date period for which the data has been extracted into the MongoDB collection.
* Next time the pipeline or the Lambda Function runs, the Lambda Function continues to extract the data, where the date/period of data extraction will continue from the last extraction date, by reading the latest date values from the MongoDB collection.
* Another Lambda Function with a trigger on Object Creation on the S3 bucket will start the Glue Job 
* The Glue job reads data from the S3 Bucket, to be Incrementally loaded into the DynamoDB Table, by simultaneously reading and filtering data that is already present in the DynamoDB table.
* Once the data load is done, the S3 data will be moved to an archive directory in S3.
* When the Glue job is successfully completed, the SNS topic sends out a Notification of job completion to the subscribers.
* The daily updated data on the DynamoDB could be used as a centralized repository for the data which could be used for further processing, analytics or application.

## API Used
[API Link](https://www.consumerfinance.gov/data-research/consumer-complaints/) is a US government-based financial consumer complaints database. The data gets updated every day in the database, where the data for the current date is delayed by 1 day.


## Requirements
* Python 3   
* AWS Services - S3, Lambda, Glue, DynamoDB, SNS, IAM and EventBridge    
* Mongo DB   
#### Python Packages - 
```python
import json
import requests
import pymongo
import boto3
import os
import datetime
```
## Use cases of data loaded into the DynamoDB Table

* We can directly query the data stored in DynamoDB to power real-time dashboards and could be used for analytics. This is possible because DynamoDB allows for fast and low-latency data access, making it suitable for real-time applications.
* DynamoDB data can be exported or integrated with various analytics tools or data warehouses for advanced analysis. We could use services like AWS Athena, AWS Redshift, or AWS QuickSight to perform complex queries, generate reports, or visualize the data.
* The financial data loaded into DynamoDB can serve as a training dataset for machine learning models. We can extract the data and use it to build and train models for predictive analytics, recommendation systems, fraud detection, or other applications.

## Improvisations

* The archived data could be stored in Parquet or ORC. This would drastically reduce file sizes and reduce storage costs.
* COALESCE - In case the use case of the dynamoDB data allows data extraction in monthly timeframe, the size of the data increases and could potentially use more than two worker node. Thus while defining filtered_sparkDf, we could bring down the number of partitions to an efficient number to optimize the Glue job.
  

## Author

[Saheen Ahzan](https://github.com/saheen619)


## Feedback

If you have any feedback, please reach out at saheen619.klm@gmail.com or [linkedin](https://www.linkedin.com/in/saheenahzan/)
