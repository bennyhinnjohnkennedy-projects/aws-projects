import boto3
import time
import csv
import io
import os
from datetime import datetime


ATHENA_DATABASE = os.environ['ATHENA_DATABASE']
ATHENA_OUTPUT_LOCATION = os.environ['ATHENA_OUTPUT_LOCATION']
S3_TARGET_BUCKET = os.environ['S3_TARGET_BUCKET']
current_date = datetime.now().strftime('%Y-%m-%d')
S3_TARGET_KEY_LOG = f'phoenix-automation/logs/contact-email-updater/contact_email_{current_date}.csv'
S3_TARGET_KEY_APPFLOW = 'phoenix-automation/appflow-data/contact-email-updater/contact_email.csv'

athena = boto3.client('athena')
s3 = boto3.client('s3')

def run(event):
    query = """select id,
billEmail.vlocity_cmt__BillingEmailAddress__c as email_to_update
from salesforce_contact cont
join (
select ff.contact_id_full,
ff.vlocity_cmt__BillingEmailAddress__c
from(
select ct.id as contract_id,
ct.status as contract_status,
c.id as contact_id_full,
substring(c.id, 1, length(c.id) - 3) as contact_id,
ct.RecordTypeName__c,
ct.Order_Number__c,
a.vlocity_cmt__BillDeliveryMethod__c,
ct.Billing_Id__c,
a.vlocity_cmt__BillingEmailAddress__c,
c.Email contact_email,
parent.vlocity_cmt__PersonContactId__c
from testathena.salesforce_contract ct
join testathena.salesforce_account a on ct.Billing_Id__c = a.id
join testathena.salesforce_account parent on a.parentid = parent.id
join testathena.salesforce_contact c on parent.vlocity_cmt__PersonContactId__c = c.id
where a.vlocity_cmt__BillDeliveryMethod__c in ('ELETTRONICO')
and ct.RecordTypeName__c in ('TV', 'BB', 'MA')
and a.vlocity_cmt__BillingEmailAddress__c is not null
and c.Email is null
and ct.Status in ('ATTIVO')
) ff
) billEmail on cont.id = billEmail.contact_id_full
where id in (
select fin.contact_id_full
from(
select ct.id as contract_id,
ct.status as contract_status,
c.id as contact_id_full,
substring(c.id, 1, length(c.id) - 3) as contact_id,
ct.RecordTypeName__c,
ct.Order_Number__c,
a.vlocity_cmt__BillDeliveryMethod__c,
ct.Billing_Id__c,
a.vlocity_cmt__BillingEmailAddress__c,
c.Email contact_email,
parent.vlocity_cmt__PersonContactId__c
from testathena.salesforce_contract ct
join testathena.salesforce_account a on ct.Billing_Id__c = a.id
join testathena.salesforce_account parent on a.parentid = parent.id
join testathena.salesforce_contact c on parent.vlocity_cmt__PersonContactId__c = c.id
where a.vlocity_cmt__BillDeliveryMethod__c in ('ELETTRONICO')
and ct.RecordTypeName__c in ('TV', 'BB', 'MA')
and a.vlocity_cmt__BillingEmailAddress__c is not null
and c.Email is null
and ct.Status in ('ATTIVO')
) fin
)
and Email is null
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
