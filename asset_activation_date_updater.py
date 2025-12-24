import boto3
import time
import csv
import io
import os
from datetime import datetime
import json

ATHENA_DATABASE = os.environ['ATHENA_DATABASE']
ATHENA_OUTPUT_LOCATION = os.environ['ATHENA_OUTPUT_LOCATION']
S3_TARGET_BUCKET = os.environ['S3_TARGET_BUCKET']
current_date = datetime.now().strftime('%Y-%m-%d')
S3_TARGET_KEY_LOG = f'phoenix-automation/logs/asset-activation-date-updater/asset_activation_date_{current_date}.csv'
S3_TARGET_KEY_APPFLOW = 'phoenix-automation/appflow-data/asset-activation-date-updater/asset_activation_date.csv'

athena = boto3.client('athena')
s3 = boto3.client('s3')

def run_athena_query_to_csv(query_string):
    # 1. Start query execution
    response = athena.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT_LOCATION}
    )

    query_execution_id = response['QueryExecutionId']

    # 2. Wait for query to complete
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)

    if status != 'SUCCEEDED':
        raise Exception(f"Query failed with status: {status}")

    # 3. Get results and write to CSV in memory
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)

    next_token = None
    row_count = 0
    header_written = False

    while True:
        if next_token:
            response = athena.get_query_results(
                QueryExecutionId=query_execution_id,
                MaxResults=1000,
                NextToken=next_token
            )
        else:
            response = athena.get_query_results(
                QueryExecutionId=query_execution_id,
                MaxResults=1000
            )

        rows = response['ResultSet']['Rows']

        # Handle header
        if not header_written and rows:
            header = [col.get('VarCharValue', '') for col in rows[0]['Data']]
            writer.writerow(header)
            header_written = True
            data_rows = rows[1:]
        else:
            data_rows = rows

        # Write data rows
        for row in data_rows:
            writer.writerow([col.get('VarCharValue', '') for col in row['Data']])
            row_count += 1

        next_token = response.get('NextToken')
        if not next_token:
            break

    print(f"Total data rows written (excluding header): {row_count}")
    return csv_buffer.getvalue(),row_count

def run(event):
    query_for_logs = """select a.id assetid, a.productcode, c.order_number__c order_number, a.status asset_Status,a.vlocity_cmt__ActivationDate__c asset_activ_date, c.StartDate contract_StartDate, a.createddate asset_createddate, rt.name recordTypeName,  DATE_FORMAT(GREATEST(DATE_PARSE(a.createddate, '%Y-%m-%d %H:%i:%s'), DATE_PARSE(c.StartDate, '%Y-%m-%d %H:%i:%s')),'%Y-%m-%d') AS date_to_update,
date_format(current_timestamp, '%Y-%m-%d %H:%i') AS update_Time
from salesforce_asset a
join salesforce_contract c on  a.childcontract__c = c.id
join salesforce_recordtype  rt on rt.id = c.recordtypeid
where a.status = 'ATTIVO'
and a.vlocity_cmt__ActivationDate__c is null
and c.status = 'ATTIVO'
and rt.name in ('TV','MA','BB')
and (DATE_PARSE(a.createddate, '%Y-%m-%d %H:%i:%s') >= DATE_PARSE(c.StartDate, '%Y-%m-%d %H:%i:%s') or c.startdate is null)
-- With this condition, we verify whether there are cases where the value of the Asset CreatedDate precedes the Contract StartDate
UNION
select a.id, a.productcode, c.order_number__c, a.status,a.vlocity_cmt__ActivationDate__c, c.StartDate, a.createddate, rt.name,DATE_FORMAT(GREATEST(DATE_PARSE(a.createddate, '%Y-%m-%d %H:%i:%s'), DATE_PARSE(c.StartDate, '%Y-%m-%d %H:%i:%s')),'%Y-%m-%d'),date_format(current_timestamp, '%Y-%m-%d %H:%i')
from salesforce_asset a
join salesforce_contract c on  a.childcontract__c = c.id
join salesforce_recordtype  rt on rt.id = c.recordtypeid
where a.status = 'ATTIVO'
and a.vlocity_cmt__ActivationDate__c is null
and c.status = 'ATTIVO'
and rt.name in ('TV','MA','BB')
and DATE_PARSE(a.createddate, '%Y-%m-%d %H:%i:%s') < DATE_PARSE(c.StartDate, '%Y-%m-%d %H:%i:%s')
    """

    query_for_appflow ="""select a.id assetid,DATE_FORMAT(GREATEST(DATE_PARSE(a.createddate, '%Y-%m-%d %H:%i:%s'), DATE_PARSE(c.StartDate, '%Y-%m-%d %H:%i:%s')),'%Y-%m-%d') AS date_to_update
from salesforce_asset a
join salesforce_contract c on  a.childcontract__c = c.id
join salesforce_recordtype  rt on rt.id = c.recordtypeid
where a.status = 'ATTIVO'
and a.vlocity_cmt__ActivationDate__c is null
and c.status = 'ATTIVO'
and rt.name in ('TV','MA','BB')
and (DATE_PARSE(a.createddate, '%Y-%m-%d %H:%i:%s') >= DATE_PARSE(c.StartDate, '%Y-%m-%d %H:%i:%s') or c.startdate is null)
UNION
select a.id,DATE_FORMAT(GREATEST(DATE_PARSE(a.createddate, '%Y-%m-%d %H:%i:%s'), DATE_PARSE(c.StartDate, '%Y-%m-%d %H:%i:%s')),'%Y-%m-%d')
from salesforce_asset a
join salesforce_contract c on  a.childcontract__c = c.id
join salesforce_recordtype  rt on rt.id = c.recordtypeid
where a.status = 'ATTIVO'
and a.vlocity_cmt__ActivationDate__c is null
and c.status = 'ATTIVO'
and rt.name in ('TV','MA','BB')
and DATE_PARSE(a.createddate, '%Y-%m-%d %H:%i:%s') < DATE_PARSE(c.StartDate, '%Y-%m-%d %H:%i:%s')
    """

    data={
        "automation_name":"asset_activation_date_updater",
        "status":"Started",
        "stack_trace":None,
        "app_flow_execution_id":None,
        "total_records":None,
        "records_completed":None,
        "records_failed":None,
        "s3_file_name":None,
        "start_date":datetime.now().isoformat(),
        "end_date":None
    }
    try:

        csv_data_for_logs, total_records = run_athena_query_to_csv(query_for_logs)
        csv_data_for_appflow, total_records = run_athena_query_to_csv(query_for_appflow)
        # 5. Upload to S3
        s3.put_object(
            Bucket=S3_TARGET_BUCKET,
            Key=S3_TARGET_KEY_LOG,
            Body=csv_data_for_logs
        )

        s3.put_object(
            Bucket=S3_TARGET_BUCKET,
            Key=S3_TARGET_KEY_APPFLOW,
            Body=csv_data_for_appflow
        )

        data["total_records"]=total_records
        data["s3_file_name"]=S3_TARGET_KEY_LOG
        data["status"]="Completed"
        data["stack_trace"]=""

        return data

    except Exception as e:
        data["status"]="Failed"
        data["stack_trace"]=str(e)
        data["total_records"]=""
        data["s3_file_name"]=""
        return data