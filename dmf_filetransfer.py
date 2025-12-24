import boto3
import paramiko
from datetime import datetime
from zoneinfo import ZoneInfo 
import os
import logging
import threading
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
import psycopg2
from psycopg2.extras import RealDictCursor
from botocore.exceptions import ClientError  # Required for S3 key check

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# Environment variables
# Number of parallel SFTP connections
MAX_THREADS = int(os.environ.get("MAX_THREADS"))  
SECRET_NAME = os.environ['DB_SECRET_NAME']
s3_bucket = os.environ['S3_BUCKET']
# s3Prefix = os.environ['s3Prefix']
# sftpTargetDir = os.environ['SFTP_TARGET_PATH']
schema = os.environ['DB_SCHEMA']
table = os.environ['DB_TABLE']
meta_table=os.environ['META_TABLE']
config_table=os.environ['CONFIG_TABLE']
transfer_limit=os.environ['TRANSFER_LIMIT']

def list_s3_files(s3_client, bucket, prefix):
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    s3_keys = []
    for page in page_iterator:
        for obj in page.get('Contents', []):
            s3_keys.append(obj['Key'])
    return s3_keys

def ensure_sftp_path_exists(sftp_client, remote_path):
    try:
        sftp_client.stat(remote_path)
        logger.info(f"Folder already exists {remote_path}")
    except FileNotFoundError:
        sftp_client.mkdir(remote_path)
        logger.info(f"Creating folder {remote_path}")

# credentials
def get_secret():
    secrets_client = boto3.client('secretsmanager', region_name='eu-west-1')
    resp = secrets_client.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(resp['SecretString'])

def get_db_credentials():
    secret = get_secret()
    db_url = secret['dbUrl'].replace('jdbc:', '')
    parsed = urlparse(db_url)
    return {
        'dbname': parsed.path.lstrip('/'),
        'user': secret['dbUsername'],
        'password': secret['dbPassword'],
        'host': parsed.hostname,
        'port': parsed.port
    }

def get_sftp_credentials():
    secret = get_secret()
    return {
        'host': secret['sftpHost'],
        'port': int(secret.get('sftpPort', 22)),
        'username': secret['sftpUsername'],
        'password': secret['sftpPassword']
    }

def get_db_connection():
    creds = get_db_credentials()
    return psycopg2.connect(
        dbname=creds['dbname'],
        user=creds['user'],
        password=creds['password'],
        host=creds['host'],
        port=creds['port']
    )

def fetch_file_list(templateType):
    if(templateType=="CONTRATTO"):
        query = f"""
            select input_file_name,cga,output_file_name from ( select *,row_number() over(partition by input_file_name order by start_date desc)
             as rn from {schema}.{table} qj where archival_status='READY' and  description like 'SIL%' and output_file_name != '' AND start_date >= CURRENT_DATE - INTERVAL '30 days' ) quill where rn = 1 limit {transfer_limit};
        """
    elif(templateType=="DIGITAL"):
        query = f"""
            select input_file_name,output_file_name,ds_mode,ds_date_time,ds_id_cgs,contract_type,tv_order_number,bb_order_number,hw_order_number,offer_type   
            from (select j.input_file_name,j.output_file_name,j.description,j.cga,d.ds_id,d.ds_mode,d.ds_date_time,d.ds_id_cgs,d.contract_type,d.tv_order_number,d.bb_order_number,d.hw_order_number,d.offer_type
            ,row_number() over(partition by j.input_file_name order by j.start_date desc) as rn from {schema}.{table} j,{schema}.{meta_table} d where j.id=d.job_id and j.archival_status='READY' and  j.description like 'Apollo%' and j.output_file_name!='' and j.start_date >= CURRENT_DATE - INTERVAL '30 days') quill where rn = 1 limit {transfer_limit};
        """
    elif(templateType=="SOAS"):
        query = f"""
            select input_file_name,contract_code_tv,work_order_number,'SOAS' as odl_type,output_file_name 
            from (select *,row_number() over(partition by j.input_file_name order by j.start_date desc) as rn from {schema}.{table} j,{schema}.{meta_table} d where j.id=d.job_id
            and j.archival_status='READY' and  j.description like 'Report%' and  j.output_file_name!='' and j.start_date >= CURRENT_DATE - INTERVAL '30 days') quill where rn = 1 limit {transfer_limit};
        """
    elif(templateType=="CGA"):
        query = f"""
            select t2.config_value AS CGA_version,t1.config_value as file_name, CONCAT(SPLIT_PART(t1.config_name , '_', 2), '/', t1.config_value) AS output_file_name
            FROM {schema}.{config_table} t1 LEFT JOIN {schema}.{config_table} t2  ON t2.config_name = CONCAT(t1.config_name, '_version') 
            where t1.active AND t1.config_name LIKE '%CGA%' and t1.config_name not like '%version%' and date(t1.created_date) = CURRENT_DATE
        """
    else:
        logger.error("Pass the correct Template Type")

    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            return cur.fetchall()

def mark_files_completed(schema, table, input_file_name, transferred_all, skipped_all):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            query = f"""
                UPDATE {schema}.{table}
                SET archival_status = 'ARCHIVED',
                    archival_time = NOW()
                WHERE archival_status = 'READY'
                AND output_file_name = ANY(%s)
            """
            # Log the query and parameters
            logger.info("Executing SQL:\n%s", query)
            logger.info("With parameters:\ninput_file_name=%s", transferred_all)

            cur.execute(query, (transferred_all,))
            conn.commit()

            query_sp = f"""
                UPDATE {schema}.{table}
                SET archival_status = 'ARCHIVAL SKIPPED',
                    archival_time = NOW()
                WHERE archival_status = 'READY'
                AND input_file_name = ANY(%s)
            """
            if skipped_all:
                query_sp += " AND output_file_name != ANY(%s)"
                params.append(skipped_all)

            # Log the actual parameters used
            logger.info("Executing SQL:\n%s", query_sp)
            logger.info("With parameters:\ninput_file_name=%s\nskipped_all=%s", input_file_name, skipped_all)

            cur.execute(query_sp, (input_file_name,))
            conn.commit()

def create_metadata_file(records, templateType):
    if templateType == "CONTRATTO":
        excluded_columns = []
        delimiter = "||" 
    elif templateType == "CGA":
        excluded_columns = ["output_file_name"]
        delimiter = "|" 
    elif templateType == "SOAS":
        excluded_columns = ["input_file_name"]
        delimiter = "#"
    elif templateType == "DIGITAL":
        excluded_columns = ["input_file_name"]
        delimiter = "#"
    else:
        delimiter = "#"
    # Extract only values from each record dict
    lines = []
    for row in records:
        filtered_row = {k: v for k, v in row.items() if k not in excluded_columns}
        line = delimiter.join("" if v is None else str(v) for v in filtered_row.values())
        lines.append(line)
    filename = "index.csv"
    return filename, "\n".join(lines)

def split_list(lst, n):
    """Split list into n nearly equal chunks."""
    k, m = divmod(len(lst), n)
    return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]

def create_sftp_connection():
    sftp_creds = get_sftp_credentials()
    transport = paramiko.Transport((sftp_creds['host'], sftp_creds['port']))
    transport.connect(username=sftp_creds['username'], password=sftp_creds['password'])
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport

def transfer_file_batch(s3_client, s3_keys, sftp_dir):
    transferred = []
    failed = []
    sftp, transport = create_sftp_connection()
    try:
        for key in s3_keys:
            filename = os.path.basename(key)
            sftp_path = os.path.join(sftp_dir, filename)
            try:
                file_obj = s3_client.get_object(Bucket=s3_bucket, Key=key)
                file_stream = file_obj['Body']
                sftp.putfo(file_stream, sftp_path)
                # logger.info(f"Transferred: {filename}")
                transferred.append(filename)
            except ClientError as e:
                if e.response['Error']['Code'] == "NoSuchKey":
                    logger.warning(f"S3 key not found, skipping: {key}")
                else:
                    logger.error(f"Failed to get object {key}: {e}", exc_info=True)
                failed.append(key)
            except Exception as e:
                logger.error(f"Failed to transfer {key}: {e}", exc_info=True)
                failed.append(key)
    finally:
        sftp.close()
        transport.close()
    return transferred,failed

def lambda_handler(event, context):
    try:

        #Event Inputs
        templateType = event.get("templateType") or "CGA"
        sftpTargetDir = event.get("sftpTargetDir") or "/cga/"
        s3Prefix = event.get("s3Prefix") or "quill/CGA/"

        logger.info(f"templateType: {templateType}")
        logger.info(f"sftpTargetDir: {sftpTargetDir}")
        logger.info(f"s3Prefix: {s3Prefix}")

        now_italy = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("Europe/Rome"))
        sftp_date_hour_folder = os.path.join(
            sftpTargetDir,
            now_italy.strftime('%Y-%m-%d-%H-%M')
        )

        s3 = boto3.client('s3')
        records = fetch_file_list(templateType)
        if not records:
            logger.info({'statusCode': 200, 'body': 'No files to transfer.'})
            return {'statusCode': 200, 'body': 'No files to transfer.'}

        output_files = [r['output_file_name'] for r in records]
        # logger.info(output_files)
        s3_keys = [os.path.join(s3Prefix, f) for f in output_files]
        # s3_keys = list_s3_files(s3, s3_bucket, s3Prefix)

        logger.info(f"s3_keys: {s3_keys}")

        # Metadata file creation
        metadata_filename, metadata_content = create_metadata_file(records,templateType)
        sftp_meta, transport_meta = create_sftp_connection()

        #Ensure SFTP File Path Exists
        ensure_sftp_path_exists(sftp_meta, sftp_date_hour_folder)

        with sftp_meta.file(os.path.join(sftp_date_hour_folder, metadata_filename), 'w') as f:
            f.write(metadata_content)
        logger.info(f"Metadata file {metadata_filename} uploaded.")
        sftp_meta.close()
        transport_meta.close()

        # Split list into chunks
        chunks = split_list(s3_keys, MAX_THREADS)

        # Run transfer in parallel
        transferred_all = []
        failed_all = []
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = [executor.submit(transfer_file_batch, s3, chunk, sftp_date_hour_folder) for chunk in chunks]
            for future in as_completed(futures):
                transferred, failed = future.result()
                transferred_all.extend(transferred)
                failed_all.extend(failed)

        # Update DB
        if transferred_all and templateType!="CGA":
            input_files = [r['input_file_name'] for r in records]
            mark_files_completed(schema, table, input_files, transferred_all, failed_all)
            logger.info("DB updated successfully.")

        # Create STARTDMS file
        sftp_dms, transport_dms = create_sftp_connection()
        try:
            with sftp_dms.file(os.path.join(sftp_date_hour_folder, "STARTDMS"), 'w') as dms_file:
                pass
            logger.info("STARTDMS file created successfully.")
        finally:
            sftp_dms.close()
            transport_dms.close()

        logger.info({
                'metadata_file': metadata_filename,
                'total_files_requested': len(s3_keys),
                'files_transferred': len(transferred_all),
                'transferred_files': transferred_all,
                'sftp_prefix': sftp_date_hour_folder,
                'failed_files': failed_all
            })
        return {
            'statusCode': 200,
            'body': json.dumps({
                'metadata_file': metadata_filename,
                'total_files_requested': len(s3_keys),
                'files_transferred': len(transferred_all),
                'transferred_files': transferred_all,
                'sftp_prefix': sftp_date_hour_folder,
                'failed_files': failed_all
            })
        }

    except Exception as e:
        logger.error(f"Lambda error: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': str(e)
        }
