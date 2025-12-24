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
S3_TARGET_KEY_LOG = f'phoenix-automation/logs/asset-product-termination-updater/contact_status_{current_date}.csv'
S3_TARGET_KEY_APPFLOW = 'phoenix-automation/appflow-data/asset-product-termination-updater/contact_status.csv'

athena = boto3.client('athena')
s3 = boto3.client('s3')

def run(event):
    query = """WITH ord_tv_last AS (
	SELECT *
	FROM (
			SELECT o.*,
				ROW_NUMBER() OVER (
					PARTITION BY o.childcontracttv__c
					ORDER BY parse_datetime(o.createddate, 'yyyy-MM-dd HH:mm:ss') DESC
				) AS rn
			FROM testathena.salesforce_order__s o
			WHERE (
					o.type IN ('CLOSE_CONTRACT', 'CONTRACT_RECONNECTION')
					OR (
						o.type = 'CHANGE_CONSISTENCY'
						AND o.OG_Action__c = 'CEASE'
					)
				)
				AND o.status IN (
					'COMPLETED',
					'Completato',
					'SUBMITTED',
					'EXECUTION',
					'Activated'
				)
		) t
	WHERE rn = 1
    ),
    ord_bb_last AS (
        SELECT *
        FROM (
                SELECT o.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY o.childcontractbb__c
                        ORDER BY parse_datetime(o.createddate, 'yyyy-MM-dd HH:mm:ss') DESC
                    ) AS rn
                FROM testathena.salesforce_order__s o
                WHERE (
                        o.type IN ('CLOSE_CONTRACT', 'CONTRACT_RECONNECTION')
                        OR (
                            o.type = 'CHANGE_CONSISTENCY'
                            AND o.OG_Action__c = 'CEASE'
                        )
                    )
                    AND o.status IN (
                        'COMPLETED',
                        'Completato',
                        'SUBMITTED',
                        'EXECUTION',
                        'Activated'
                    )
            ) t
        WHERE rn = 1
    ),

    cease_active_c AS (SELECT
        --  Conteggio asset coerenti con il tipo di contratto
        COUNT(
            DISTINCT CASE 
            WHEN c.recordtypename__c = 'TV'
            AND (ass.RootProductSpec__c <> 'TELCO' or ass.RootProductSpec__c is null ) THEN ass.id
            WHEN c.recordtypename__c = 'BB'
            AND ass.RootProductSpec__c = 'TELCO' THEN ass.id
            ELSE 'NA' 
            END
        ) AS numero_asset_attivi,
        c.id AS contratto_id,
        c.order_number__c,
        c.data_cessazione_dt__c,
        c.recordtypename__c,
        tv.type AS type_tv,
        tv.status AS status_tv,
        tv.OG_Action__c AS og_action_tv,
        bb.type AS type_bb,
        bb.status AS status_bb,
        bb.OG_Action__c AS og_action_bb,
        format_datetime(
            parse_datetime(tv.createddate, 'yyyy-MM-dd HH:mm:ss'),
            'yyyy-MM-dd''T''HH:mm:ss.SSSZ'
        ) AS createddate_tv,
        format_datetime(
            parse_datetime(bb.createddate, 'yyyy-MM-dd HH:mm:ss'),
            'yyyy-MM-dd''T''HH:mm:ss.SSSZ'
        ) AS createddate_bb

    FROM testathena.salesforce_contract c
        JOIN testathena.salesforce_asset ass ON ass.childcontract__c = c.id
        JOIN testathena.salesforce_recordtype contract_rt ON c.recordtypeid = contract_rt.id
        LEFT JOIN ord_tv_last tv ON c.id = tv.childcontracttv__c
        AND tv.offertype__c = contract_rt.name
        LEFT JOIN ord_bb_last bb ON c.id = bb.childcontractbb__c
        AND bb.offertype__c = contract_rt.name
    WHERE c.status = 'CESSATO'
        AND ass.status = 'ATTIVO'
        AND ass.ProductCode NOT LIKE '%PENAL%'
        AND ass.ProductCode not in  ('EXIT_FEE_TV','COSTI_DI_DISATTIVAZIONE_TV','COSTI_DI_CESSAZIONE_BB','PENALE_MANCATO_RESO_STB','PENALE_MANCATO_RESO_DK')
        AND c.recordtypename__c NOT IN ('HW')
        
    GROUP BY c.id,
        c.order_number__c,
        c.data_cessazione_dt__c,
        c.recordtypename__c,
        tv.type,
        tv.status,
        tv.OG_Action__c,
        tv.createddate,
        bb.type,
        bb.status,
        bb.OG_Action__c,
        bb.createddate
    ORDER BY numero_asset_attivi DESC),

    q_tv_ma as (select id, ProductCode, status, 
    vlocity_cmt__ProvisioningStatus__c, 
    vlocity_cmt__DisconnectDate__c, 
    vlocity_cmt__Action__c,
    childcontract__c
    from salesforce_asset
    where ProductCode not like '%PENAL%'
    AND ProductCode not in  ('EXIT_FEE_TV','COSTI_DI_DISATTIVAZIONE_TV',
        'COSTI_DI_CESSAZIONE_BB','COSTI_DI_MIGRAZIONE_BB',
        'COSTI_RIATTIVAZIONE_BB')
    -- and childcontract__c in ('contratto_id’)
        
    and (RootProductSpec__c  not in  ('TELCO','LLAMA') OR RootProductSpec__c is null)
    and status in ('ATTIVO') ),

    q_bb as (select id, ProductCode, status, 
    vlocity_cmt__ProvisioningStatus__c, 
    vlocity_cmt__DisconnectDate__c, 
    vlocity_cmt__Action__c,
    childcontract__c
    from salesforce_asset
    where ProductCode not like '%PENAL%'
    AND ProductCode not in  ('EXIT_FEE_TV','COSTI_DI_DISATTIVAZIONE_TV',
        'COSTI_DI_CESSAZIONE_BB','COSTI_DI_MIGRAZIONE_BB',
        'COSTI_RIATTIVAZIONE_BB')
    -- and childcontract__c in ('contratto_id')

    and RootProductSpec__c  in  ('TELCO')
    and status in ('ATTIVO'))

    select id,'CESSATO' as status, 
    'Deleted' as vlocity_cmt__ProvisioningStatus__c, 
    createddate_bb as vlocity_cmt__DisconnectDate__c, 
    'Disconnect' as vlocity_cmt__Action__c from cease_active_c
    join q_bb on cease_active_c.contratto_id=q_bb.childcontract__c and cease_active_c.recordtypename__c ='BB' 
    union all 
    select id,'CESSATO' as status, 
    'Deleted' as vlocity_cmt__ProvisioningStatus__c, 
    createddate_tv as vlocity_cmt__DisconnectDate__c, 
    'Disconnect' as vlocity_cmt__Action__c from cease_active_c
    join q_tv_ma on cease_active_c.contratto_id=q_tv_ma.childcontract__c and cease_active_c.recordtypename__c in ('TV','MA') 
    """
    data={
        "id":f"asset_activation_{current_date}",
        "automation_name":"asset_product_termination_updater",
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
