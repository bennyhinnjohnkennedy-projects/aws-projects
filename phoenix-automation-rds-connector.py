import json
import boto3
import logging
import psycopg2
import os
import io
from urllib.parse import urlparse
from botocore.exceptions import ClientError
from datetime import datetime
import csv


def default_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)

# Logging setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuration
SECRET_NAME = os.environ['SECRET_NAME']
SCHEMA_NAME = os.environ['SCHEMA_NAME']
TABLE_NAME  = os.environ['TABLE_NAME']
S3_BUCKET = os.environ['S3_TARGET_BUCKET']

# Boto3 client for Secrets Manager
secrets_client = boto3.client('secretsmanager', region_name='eu-west-1')

def get_db_credentials():
    try:
        secrets_client = boto3.client('secretsmanager', region_name='eu-west-1')
        resp = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        secret = json.loads(resp['SecretString'])

        db_url = secret['dbUrl'].replace('jdbc:', '')
        parsed = urlparse(db_url)
        return {
            'dbname': parsed.path.lstrip('/'),
            'user': secret['dbUsername'],
            'password': secret['dbPassword'],
            'host': parsed.hostname,
            'port': parsed.port
        }
    except ClientError as e:
        logger.error(f"Could not retrieve secret {SECRET_NAME}: {e}")
        raise

def get_db_connection():
    """
    Returns a new psycopg2 connection using credentials
    retrieved from Secrets Manager.
    """
    creds = get_db_credentials()
    return psycopg2.connect(
        dbname   = creds['dbname'],
        user     = creds['user'],
        password = creds['password'],
        host     = creds['host'],
        port     = creds['port']
    )

def update_db(data,automation_name,record_id,total_records,failure_rows):
    if failure_rows==0 and data[0]['executionStatus']=='Successful':
        last_updated = data[0]['lastUpdatedAt'].isoformat()
        records_processed = data[0]['executionResult']['recordsProcessed']
        execution_status = data[0]['executionStatus']
        execution_id = data[0]['executionId']
        passed_records = data[0]['executionResult']['recordsProcessed']
        # Connect to the database
        conn = get_db_connection()
        cur = conn.cursor()
        # Update the database
        cur.execute(f"UPDATE {SCHEMA_NAME}.{TABLE_NAME} SET end_date = '{last_updated}', total_records = {records_processed},status='COMPLETED',stack_trace='{execution_status}',execution_id='{execution_id}',passed_records={passed_records} WHERE id = {record_id}")
        conn.commit()
        cur.close()
        conn.close()
        return True
    else:
        last_updated = data[0]['lastUpdatedAt'].isoformat()
        records_processed = data[0]['executionResult']['recordsProcessed']
        execution_id = data[0]['executionId']
        passed_records = records_processed-failure_rows
        if passed_records == 0:
            status = 'FAILURE'
        else:
            status = 'PARTIAL_FAILURE'
        # Connect to the database
        conn = get_db_connection()
        cur = conn.cursor()
        # Update the database
        cur.execute(f"UPDATE {SCHEMA_NAME}.{TABLE_NAME} SET end_date = '{last_updated}', total_records = {records_processed},status='{status}',stack_trace='Failure at Appflow, please check the CSV',execution_id='{execution_id}',passed_records={passed_records},failed_records={failure_rows} WHERE id = {record_id}")
        conn.commit()
        cur.close()
        conn.close()
        return True

def checkForPartialFailure(execution_id):
    s3 = boto3.client('s3')
    folder_prefix = 'phoenix-automation/error-logs//'+execution_id
    folder_prefix = folder_prefix+'/'
    # List objects with the prefix
    response = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=folder_prefix,
        MaxKeys=1  # We only need to know if at least one object exists
    )

    if 'Contents' in response:
        content_key = response['Contents'][0]['Key']
        response_for_file = s3.get_object(Bucket=S3_BUCKET, Key=content_key)
        content = response_for_file['Body'].read().decode('utf-8')
        print('content--', content)
        failed_records = []
        headers_set = set()

        for line in content.strip().splitlines():
            try:
                item = json.loads(line)
                # Loop inside errorDetails list
                for err_detail in item.get('errorDetails', []):
                    record_str = err_detail.get('record')
                    error_str = err_detail.get('error')

                    if not record_str or record_str == "null":
                        continue

                    record = json.loads(record_str)
                    error_list = json.loads(error_str)  # list of error dicts

                    for error_obj in error_list:
                        combined = {**record, **error_obj}  # merge record + error into one dict
                        failed_records.append(combined)
                        headers_set.update(combined.keys())
            except Exception as e:
                print(f"Failed to parse line: {e}")

        num_failed = len(failed_records)
        print(f"Number of failed records: {num_failed}")

        # Sort headers for consistency
        headers = sorted(headers_set)

        # Optionally: Convert to CSV in memory
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=headers)
        writer.writeheader()
        for record in failed_records:
            writer.writerow(record)

        csv_content = output.getvalue()
        file_name = f"failed_records_{execution_id}.csv"

        # Optional: Upload CSV to S3
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f'phoenix-automation/error-logs/{file_name}',
            Body=csv_content.encode('utf-8'),
            ContentType='text/csv'
        )

        return{
            "statusCode": 500,
            "num_rows": num_failed
        }
    else:
        return{
            "statusCode": 200,
            "num_rows": 0
        }

def lambda_handler(event, context):
    # TODO implement
    res={}
    automation_name=event.get("body", {}).get("automation_name")
    res["automation_name"]=automation_name
    execution_id = event.get("body", {}).get("execution_id")
    res["execution_id"]=execution_id
    record_id= event.get("body", {}).get("id")
    res["id"]=record_id
    total_records = event.get("body", {}).get("total_records")
    res["total_records"]=total_records

    client = boto3.client('appflow')
    try:
        response = client.describe_flow_execution_records(
            flowName=automation_name,
            maxResults=10  # optional
        )

        # Filter by executionId if provided
        print("Looking for execution ID:", execution_id)
        if execution_id:
            matching_runs = [
                record for record in response['flowExecutions']
                if record['executionId'] == execution_id
            ]
        else:
            matching_runs = response['flowExecutions']

        if matching_runs[0]['executionStatus']=='InProgress':
            return {
                "statusCode": 200,
                "execution_state": "InProgress",
                "body":res
            }
        else:
            partial_failure=checkForPartialFailure(execution_id)
            if partial_failure['statusCode']==500:
                if total_records!=partial_failure:
                    failure_rows=total_records-partial_failure['num_rows']
                else:
                    failure_rows=partial_failure['num_rows']
                update_result=update_db(matching_runs,automation_name,record_id,total_records,failure_rows)
                json_string = json.dumps(matching_runs, default=default_serializer)
                return {
                "statusCode": 500,
                "successes": json_string,
                "execution_state":"Failure"
            }
            else:
                update_result=update_db(matching_runs, automation_name, record_id,total_records,0)
                json_string = json.dumps(matching_runs, default=default_serializer)
                return {
                    "statusCode": 200,
                    "successes": json_string,
                    "execution_state":"Successful"
                }

    except Exception as e:
        return {
            "statusCode": 500,
            "error": str(e)
        }