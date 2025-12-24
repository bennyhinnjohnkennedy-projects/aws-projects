import json
import boto3
import os
import base64
import time
from datetime import datetime, timezone,timedelta
import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_der_private_key


secrets_client = boto3.client('secretsmanager', region_name='eu-west-1')
CLIENT_ID = ''
USERNAME = ''
LOGIN_URL = ''
APPFLOW_SECRET_NAME = os.environ.get('APPFLOW_SECRET_NAME')
SECRET_NAME = os.environ['SECRET_NAME']
PRIVATE_KEY_CONTENT=''

def setSecrets():
    try:
        secrets_client = boto3.client('secretsmanager', region_name='eu-west-1')
        resp = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        secret = json.loads(resp['SecretString'])
        global CLIENT_ID
        CLIENT_ID = secret.get('SALESFORCE_CLIENT_ID')
        global USERNAME
        USERNAME = secret.get('SALESFORCE_USERNAME')
        global LOGIN_URL
        LOGIN_URL = secret.get('SALESFORCE_LOGIN_URL')
        global PRIVATE_KEY_CONTENT
        PRIVATE_KEY_CONTENT= secret.get('SALESFORCE_PRIVATE_KEY')

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


def checkForConnections():
    try:
        secrets_client = boto3.client('secretsmanager', region_name='eu-west-1')
        resp = secrets_client.get_secret_value(SecretId=APPFLOW_SECRET_NAME)
        secret = json.loads(resp['SecretString'])
        lastUpdatedAt = secret.get('lastUpdated')
        return lastUpdatedAt

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


def generateToken():
    try:
        private_key = get_private_key()
        # Create JWT payload (2-minute expiration like your Java code)
        now = datetime.utcnow()
        payload = {
            'iss': CLIENT_ID,
            'sub': USERNAME,
            'aud': LOGIN_URL,
            'exp': int((now + timedelta(minutes=120)).timestamp())
        }

        # Generate JWT token using the private key object
        return jwt.encode(payload, private_key, algorithm='RS256')
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def updateconnection(token):
    try:
        # Get current secret value
        response = secrets_client.get_secret_value(SecretId=APPFLOW_SECRET_NAME)
        current_secret = json.loads(response['SecretString'])

        # Update only the JWT token value and lastupdated time
        current_secret['jwtToken'] = token
        current_secret['lastUpdated'] = datetime.utcnow().isoformat()

        # Update the secret with modified values
        secrets_client.update_secret(
            SecretId=APPFLOW_SECRET_NAME,
            SecretString=json.dumps(current_secret)
        )

        return {'status': 'success', 'message': f'JWT updated in {APPFLOW_SECRET_NAME}'}

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


def get_private_key():
    """Load private key from secrets"""

    try:
        if PRIVATE_KEY_CONTENT:
            # Clean the key content like your Java code does
            private_key_clean = PRIVATE_KEY_CONTENT.replace("-----BEGIN PRIVATE KEY-----", "")\
                                                  .replace("-----END PRIVATE KEY-----", "")\
                                                  .replace("\n", "")\
                                                  .replace(" ", "")

            # Decode the base64 content
            key_bytes = base64.b64decode(private_key_clean)

            # Load the private key using cryptography library
            private_key = serialization.load_der_private_key(
                key_bytes,
                password=None
            )
            return private_key

        raise ValueError("PRIVATE_KEY_CONTENT not found in environment variables")

    except Exception as e:
        raise Exception(f"Error loading private key: {str(e)}")

def lambda_handler(event, context):
    try:
        #set the secrets
        setSecrets()

        #check the token last updated time
        lastUpdatedAt = checkForConnections()

        if isinstance(lastUpdatedAt, dict) and lastUpdatedAt.get('statusCode') == 500:
            raise ValueError("Error in getting last updated time")

        hours_difference = 2
        if lastUpdatedAt and lastUpdatedAt!='':
                currentTimestamp = datetime.now().isoformat()
                timestamp1 = datetime.fromisoformat(currentTimestamp)
                timestamp2 = datetime.fromisoformat(lastUpdatedAt)
                time_difference = timestamp1 - timestamp2
                hours_difference = time_difference.total_seconds() / 3600

        #generating a new tokken if the token is created before 90 mins
        if hours_difference < 1.5:
            print("Connection is up to date")
        else:
            token=generateToken()
            if isinstance(token, dict) and token.get('statusCode') == 500:
                raise ValueError("Error in generating token")

            updatedDetails=updateconnection(token)
            if isinstance(token, dict) and token.get('statusCode') == 500:
                raise ValueError("Error in updating secrets")

            print("New Token Generated and updated in Secrets Manager")

        #Start the appflow
        body = event.get("body", {})
        automation_name = body.get("automation_name")
        total_records = body.get("total_records")
        record_id = body.get("id")

        if not automation_name:
            raise ValueError("Missing 'automation_name' in input body")

        # Initializing the AppFlow client
        appflow = boto3.client("appflow")

        # Triggering AppFlow flow
        response = appflow.start_flow(flowName=automation_name)
        execution_id = response.get("executionId", "unknown")

        # Constructing success response
        result = {
            "statusCode": 200,
            "body": {
                "execution_id": execution_id,
                "total_records": total_records,
                "automation_name": automation_name,
                "id": record_id
            }
        }

        print("Lambda output:", json.dumps(result))
        return result

    except Exception as e:
        error_response = {
            "statusCode": 500,
            "body": {
                "error_message": str(e),
                "input": event
            }
        }

        print("Error:", json.dumps(error_response))
        return error_response