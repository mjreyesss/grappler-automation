import azure.functions as func
import paramiko
import os
from dotenv import load_dotenv
from databricks import sql
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from io import StringIO, BytesIO
import pandas as pd
from datetime import datetime
import logging
import pysftp
import pytz
import io

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

SFTP_HOST = "deltaintegrationstorage.blob.core.windows.net"
SFTP_USERNAME = "deltaintegrationstorage.mjuser"

def get_current_date():
    nz_tz = pytz.timezone('Pacific/Auckland')
    nz_time = datetime.now(nz_tz)
    return nz_time.strftime("%d-%m-%Y")

def fetch_data_from_databricks(query, connection):
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            result = cursor.fetchall()
        return pd.DataFrame(result, columns=columns)
    except Exception as e:
        logger.error(f"Error fetching data from Databricks: {str(e)}", exc_info=True)
        raise

def main(mytimer: func.TimerRequest) -> None:
    """
    This function runs on a timer schedule defined in function.json
    """
    utc_timestamp = datetime.utcnow().replace(tzinfo=pytz.utc).isoformat()
    
    if mytimer.past_due:
        logger.info('The timer is past due!')
    
    logger.info(f'Python timer trigger function ran at {utc_timestamp}')
    
    try:
        # Your existing logic
        query = f"""SELECT * FROM delta_datawarehouse_merger.deltadbprodnz.policy WHERE id = 1"""
        
        # Connect to Databricks
        connection = sql.connect(
            server_hostname=os.getenv('DATABRICKS_SERVER_HOSTNAME'),
            http_path=os.getenv('DATABRICKS_HTTP_PATH'),
            access_token=os.getenv('DATABRICKS_TOKEN')
        )
        
        try:
            df = fetch_data_from_databricks(query, connection)
            logger.info(f"Fetched {len(df)} rows from Databricks")
        finally:
            connection.close()
        
        # Generate file
        current_date = get_current_date()
        blob_name = f'grapplerau_delta_{current_date}.csv'
        df['file_name'] = blob_name
        
        # Save to temporary location
        temp_file_path = f'/tmp/grapplerau_delta_{current_date}.csv'
        df.to_csv(temp_file_path, index=False)
        logger.info(f"Saved CSV to {temp_file_path}")
        
        # SFTP upload
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        private_key = paramiko.RSAKey.from_private_key(io.StringIO(os.getenv("OPENSSH_KEY")))
        ssh.connect(SFTP_HOST, username=SFTP_USERNAME, pkey=private_key)
        
        sftp = ssh.open_sftp()
        sftp.put(temp_file_path, f'grapplerau_delta_{current_date}.csv')
        sftp.close()
        ssh.close()
        
        # Clean up
        os.remove(temp_file_path)
        
        logger.info(f"Successfully uploaded {blob_name} to SFTP")
        
    except Exception as e:
        logger.error(f"Function execution failed: {str(e)}", exc_info=True)
        raise