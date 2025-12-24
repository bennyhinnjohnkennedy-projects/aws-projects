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
S3_TARGET_KEY_LOG = f'phoenix-automation/logs/contract-termination-reason-updater/contract_termination_reason_{current_date}.csv'
S3_TARGET_KEY_APPFLOW = 'phoenix-automation/appflow-data/contract-termination-reason-updater/contract_termination_reason.csv'

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
    query = """
      WITH contratti AS ( --contratti già filtrati
  SELECT
      c.id AS contratto_id,
      c.order_number__c,
      c.status AS contract_status,
      c.recordtypename__c,
      c.causale_cessazione__c,
      c.data_cessazione_dt__c,
      c.data_richiesta_cessazione_dt__c,
      rt.name                          AS rt_name
  FROM salesforce_contract c
  JOIN salesforce_recordtype rt
    ON rt.id = c.recordtypeid
  WHERE c.status = 'CESSATO'
    AND c.causale_cessazione__c IS NULL
    AND c.recordtypename__c IN ('BB','TV','MA')
),
ordini_base AS (
  SELECT
      o.id,
      o.type,
      o.status,
      o.og_action__c,
      format_datetime(parse_datetime(o.createddate, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd''T''HH:mm:ss.SSSZ') as created_at,
      o.og_schedulateddate__c,
      o.offertype__c,
      o.childcontracttv__c AS contract_id
  FROM salesforce_order__s o
  WHERE o.childcontracttv__c IS NOT NULL
  AND ( o.type IN ('CLOSE_CONTRACT','CONTRACT_RECONNECTION') OR (o.type = 'CHANGE_CONSISTENCY' AND o.og_action__c = 'CEASE') )
    AND o.status IN ('COMPLETED','Completato','SUBMITTED','EXECUTION','Activated')
  UNION ALL
  SELECT
      o.id,
      o.type,
      o.status,
      o.og_action__c,
      format_datetime(parse_datetime(o.createddate, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd''T''HH:mm:ss.SSSZ') as created_at,
      o.og_schedulateddate__c,
      o.offertype__c,
      o.childcontractbb__c AS contract_id
  FROM salesforce_order__s o
  WHERE o.childcontractbb__c IS NOT NULL
  AND ( o.type IN ('CLOSE_CONTRACT','CONTRACT_RECONNECTION') OR (o.type = 'CHANGE_CONSISTENCY' AND o.og_action__c = 'CEASE') )
    AND o.status IN ('COMPLETED','Completato','SUBMITTED','EXECUTION','Activated')
),
ordini_ranked AS (
  SELECT
      ob.*,
      ROW_NUMBER() OVER (PARTITION BY ob.contract_id ORDER BY ob.created_at DESC NULLS LAST) AS rn
  FROM ordini_base ob
),
ordini_latest AS (
  SELECT *
  FROM ordini_ranked
  WHERE rn = 1
)
SELECT
    c.contratto_id as id,
    'BONIFICA DA IT' AS causale_cessazione__c,
    c.order_number__c,
    c.contract_status,
    c.recordtypename__c,
    c.data_cessazione_dt__c,
    c.data_richiesta_cessazione_dt__c,
    o.id  AS ordine_id,
    o.type  AS ordine_tipo,
    o.status  AS ordine_status,
    o.created_at AS ordine_created_at,
    o.og_schedulateddate__c
FROM contratti c
JOIN ordini_latest o
  ON o.contract_id = c.contratto_id
 AND o.offertype__c = c.rt_name
 AND ( o.type = 'CLOSE_CONTRACT' OR (o.type = 'CHANGE_CONSISTENCY' AND o.og_action__c = 'CEASE') )
 AND o.status IN ('COMPLETED','Completato') limit 5
    """

    data={
        "id":f"asset_activation_{current_date}",
        "automation_name":"contact_email_updater",
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
        # 1. Start Athena query
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': ATHENA_DATABASE},
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT_LOCATION}
        )

        query_execution_id = response['QueryExecutionId']

        # 2. Wait for completion
        while True:
            status = athena.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(2)

        if status != 'SUCCEEDED':
            raise Exception(f"Query failed with status: {status}")

        # 3. Get results
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
                NextToken=next_token )
            else:
                response = athena.get_query_results(
                QueryExecutionId=query_execution_id,
                MaxResults=1000)

            rows = response['ResultSet']['Rows']

            # First row in the result is header
            if not header_written and rows:
            # Write header row
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
        total_records=row_count
        #5. Upload to S3
        s3.put_object(
            Bucket=S3_TARGET_BUCKET,
            Key=S3_TARGET_KEY_LOG,
            Body=csv_buffer.getvalue()
        )

        s3.put_object(
            Bucket=S3_TARGET_BUCKET,
            Key=S3_TARGET_KEY_APPFLOW,
            Body=csv_buffer.getvalue()
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