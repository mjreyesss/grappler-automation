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
import logging

load_dotenv()

SFTP_HOST = "deltaintegrationstorage.blob.core.windows.net"
SFTP_USERNAME = "deltaintegrationstorage.mjuser"  # storageaccount.username format

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



query = f"""  SELECT * FROM delta_datawarehouse_merger.deltadbprodnz.policy WHERE id = 1 """
try:
    connection = sql.connect(
    server_hostname=os.getenv('DATABRICKS_SERVER_HOSTNAME'),
    http_path=os.getenv('DATABRICKS_HTTP_PATH'),
    access_token=os.getenv('DATABRICKS_TOKEN')
    )
    df = fetch_data_from_databricks(query, connection)
finally:
    connection.close()


current_date = get_current_date()
blob_name = f'grapplerau_delta_{current_date}.csv'
df['file_name'] = blob_name
df.to_csv(f'grapplerau_delta_{current_date}.csv', index=False)

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    # Option 1: Load private key from file
private_key = paramiko.RSAKey.from_private_key(io.StringIO(os.getenv("OPENSSH_KEY")))


ssh.connect(SFTP_HOST, username=SFTP_USERNAME, pkey=private_key)
    
sftp = ssh.open_sftp()
sftp.put(f'grapplerau_delta_{current_date}.csv', f'grapplerau_delta_{current_date}.csv')
sftp.close()
ssh.close()

# Clean up local file
os.remove(f'grapplerau_delta_{current_date}.csv')

