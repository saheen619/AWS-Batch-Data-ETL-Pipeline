import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import os
import boto3
from pyspark.sql import functions
from pyspark.sql.types import IntegerType
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Declaring constant Variables
BUCKET_NAME = "consumer-compliants-data"
DYNAMODB_TABLE = "consumer-complaints-database"
INPUT_FILE_PATH = f"s3://{BUCKET_NAME}/inbox/*.json"

# Getting logger object to log the progress of the job
logger = glueContext.get_logger()

logger.info(f"Started reading json file from the {INPUT_FILE_PATH}")
sparkDf = spark.read.json(INPUT_FILE_PATH)

# We will be doing operations on the sparkDf with column complaint_id, so, to have complain_id data to be understood by pyspark, we typecast the column to IntegerType 
logger.info(f"Typecasting the complaint_id column to IntegerType datatype")
sparkDf = sparkDf.withColumn("complaint_id",functions.col("complaint_id").cast(IntegerType()))

logger.info(f"Columns in the incoming spark dataframe : {len(sparkDf.columns)}--> {sparkDf.columns}")
logger.info(f"Number of records in the incoming spark dataframe : {sparkDf.count()}")

# Accessing dynamodb data by using a dynamodb dynamic dataframe
dynamodb_Dyf = glueContext.create_dynamic_frame.from_options(
    connection_type = "dynamodb",
    connection_options = {"dynamodb.input.tableName" : DYNAMODB_TABLE,
        "dynamodb.throughput.read.percent" : "1.0",
        "dynamodb.splits" : "8"
        }
    )

# Converting the dynamic dataframe to spark dataframe
Dyf_sparkDf = dynamodb_Dyf.toDF()
filtered_sparkDf = None

if Dyf_sparkDf.count()!=0:
    logger.info(f"Columns in the dynamodb dynamic frame : {len(Dyf_sparkDf.columns)}--> {Dyf_sparkDf.columns}")
    logger.info(f"Number of records in the dynamodb dynamic frame : {Dyf_sparkDf.count()}")
    
    # Since complaint_id column is used as the partition key in DynamoDB, we use it for the join operation
    logger.info(f"Creating a new Df and renaming complaint_id column in Dyf_sparkDf")
    existing_data_sparkDf = Dyf_sparkDf.select("complaint_id").withColumnRenamed("complaint_id", "existing_complaint_id")
    
    logger.info(f"Applying left join to filter out duplicate records from sparkDf (s3 data) ")
    joined_sparkDf = sparkDf.join(existing_data_sparkDf, sparkDf.complaint_id == existing_data_sparkDf.existing_complaint_id, "left" )
    
    # While joining, all the duplicate records get accumilated on the left join side and the records with the Null values are the records we need to push to the dynamodb.
    filtered_sparkDf = joined_sparkDf.filter(functions.col("existing_complaint_id").isNull())
    #filtered_sparkDf = joined_sparkDf.filter("existing_complaint_id is null")
    # Dropping the unwanted column existing_complaint_id
    filtered_sparkDf.drop("existing_complaint_id")
    
# while reading the data from dynamodb for the first time, there is a possibility that there is no data yet in dynamodb table. in that case, we assign sparkDf directly to filtered_sparkDf
else:
    filtered_sparkDf = sparkDf
    
# Now since we want to store the filtered_sparkDf to dynamodb, we have to convert filtered_sparkDf to a Dynamic frame
logger.info(f"converting filtered_sparkDf to Dynamic Frame")
filtered_Dyf = DynamicFrame.fromDF(filtered_sparkDf, glueContext)

logger.info(f"Started writing new records into dynamodb table {DYNAMODB_TABLE}")
logger.info(f"Number of records to be pushed to dynamodb {filtered_sparkDf.count()}")
glueContext.write_dynamic_frame_from_options(
    frame = filtered_Dyf,
    connection_type = "dynamodb",
    connection_options = {"dynamodb.output.tableName" : DYNAMODB_TABLE,
        "dynamodb.throughput.percent" : "1.0"
    }
)
logger.info(f"data has been dumped into dynamodb table {DYNAMODB_TABLE}")

# Once the json files from s3 bucket has been processed, we move the json files to an archive directory in the same bucket
logger.info(f"Archiving processed files from inbox directory: s3://{BUCKET_NAME}/inbox/ to archive: s3://{BUCKET_NAME}/archive/ ")
os.system(f"aws s3 sync s3://{BUCKET_NAME}/inbox/ s3://{BUCKET_NAME}/archive/")
os.system(f"aws s3 rm s3://{BUCKET_NAME}/inbox/ --recursive")


# Create an SNS client to Publish a message to an SNS topic
logger.info(f"Sending Successful job completion notification to the subscribers")

try:
    sns_client = boto3.client("sns")
    topic_arn = "<SNS topic ARN>"
    message = "Hello, the consumer complaints ETL job has been successfully completed"
    subject = 'Glue ETL Job NOTIFICATION'

    response = sns_client.publish(
        TopicArn=topic_arn,
        Message=message,
        Subject=subject
    )
    logger.info(f"SNS notification to sub's was successful")
except Exception as err:
    logger.info(f"SNS notification to sub's not successful : {str(err)}")

job.commit()