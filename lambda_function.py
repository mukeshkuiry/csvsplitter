import logging
import pandas as pd
import boto3
from datetime import datetime
from dateutil.parser import parse
from io import StringIO

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='Time : %(asctime)s || Type:%(levelname)s || Msg:%(message)s'
)

# Initialize S3 client
s3_client = boto3.client('s3')

def split_csv(bucket_name, file_key, dob_cutoff, output_bucket):
    try:
        # Download the CSV file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_csv(response['Body'])

        # Convert 'Date of birth' column to datetime
        df['Date of birth'] = pd.to_datetime(df['Date of birth'], errors='coerce')

        # Parse the user-provided DoB cutoff date
        dob_cutoff = parse(dob_cutoff)

        # Split the data into two data frames based on the cutoff date
        below_cutoff = df[df['Date of birth'] < dob_cutoff]
        above_cutoff = df[df['Date of birth'] >= dob_cutoff]

        # Save the resulting data frames to new CSV files in memory
        below_cutoff_csv = StringIO()
        above_cutoff_csv = StringIO()
        below_cutoff.to_csv(below_cutoff_csv, index=False)
        above_cutoff.to_csv(above_cutoff_csv, index=False)

        # Reset the file pointers
        below_cutoff_csv.seek(0)
        above_cutoff_csv.seek(0)

        # Upload the results back to S3
        below_cutoff_key = 'customers_below_cutoff.csv'
        above_cutoff_key = 'customers_above_cutoff.csv'

        s3_client.put_object(Bucket=output_bucket, Key=below_cutoff_key, Body=below_cutoff_csv.getvalue())
        s3_client.put_object(Bucket=output_bucket, Key=above_cutoff_key, Body=above_cutoff_csv.getvalue())

        logging.info("Successfully split the CSV file and uploaded the results to S3.")
    except Exception as e:
        logging.error(f"Error processing {file_key} from bucket {bucket_name}: {e}")
        raise

def lambda_handler(event, context):
    try:
        # Extract parameters from the event object
        bucket_name = event['bucket_name']
        file_key = event['file_key']
        dob_cutoff = event['dob_cutoff']
        output_bucket = event.get('output_bucket', bucket_name)  # Default to the same bucket if not provided

        # Split the CSV file
        split_csv(bucket_name, file_key, dob_cutoff, output_bucket)

        logging.info("Lambda function completed successfully.")
        return {
            'statusCode': 200,
            'body': 'CSV processing completed successfully with happy ending :)'
        }
    except Exception as e:
        logging.error(f"Lambda function failed: {e}")   
        return {
            'statusCode': 500,
            'body': f"Lambda function failed: {e}"
        }