# import azure.functions as func
# import paramiko
# import os
# from dotenv import load_dotenv
# from databricks import sql
# from azure.storage.blob import BlobServiceClient
# from azure.keyvault.secrets import SecretClient
# from azure.identity import DefaultAzureCredential
# from io import StringIO, BytesIO
# import pandas as pd
# from datetime import datetime
# import logging
# import pysftp
# import pytz
# import io

# # Set up logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# load_dotenv()

# SFTP_HOST = "deltaintegrationstorage.blob.core.windows.net"
# SFTP_USERNAME = "deltaintegrationstorage.mjuser"

# def get_current_date():
#     nz_tz = pytz.timezone('Pacific/Auckland')
#     nz_time = datetime.now(nz_tz)
#     return nz_time.strftime("%d-%m-%Y")

# def fetch_data_from_databricks(query, connection):
#     try:
#         with connection.cursor() as cursor:
#             cursor.execute(query)
#             columns = [desc[0] for desc in cursor.description]
#             result = cursor.fetchall()
#         return pd.DataFrame(result, columns=columns)
#     except Exception as e:
#         logger.error(f"Error fetching data from Databricks: {str(e)}", exc_info=True)
#         raise

# def main(mytimer: func.TimerRequest) -> None:
#     """
#     This function runs on a timer schedule defined in function.json
#     """
#     # Force immediate output
#     print("=== FUNCTION STARTING ===")
#     logging.info("=== FUNCTION STARTING ===")
    
#     utc_timestamp = datetime.utcnow().replace(tzinfo=pytz.utc).isoformat()
    
#     if mytimer.past_due:
#         print('The timer is past due!')
#         logging.info('The timer is past due!')
    
#     print(f'Python timer trigger function ran at {utc_timestamp}')
#     logging.info(f'Python timer trigger function ran at {utc_timestamp}')
    
#     try:
#         print("=== STARTING DATABRICKS CONNECTION ===")
#         logging.info("=== STARTING DATABRICKS CONNECTION ===")
        
#         # Your existing logic
#         query = f"""SELECT * FROM delta_datawarehouse_merger.deltadbprodnz.policy WHERE id = 1"""
        
#         print(f"Query: {query}")
#         logging.info(f"Query: {query}")
        
#         # Connect to Databricks
#         print("Connecting to Databricks...")
#         logging.info("Connecting to Databricks...")
        
#         connection = sql.connect(
#             server_hostname=os.getenv('DATABRICKS_SERVER_HOSTNAME'),
#             http_path=os.getenv('DATABRICKS_HTTP_PATH'),
#             access_token=os.getenv('DATABRICKS_TOKEN')
#         )
        
#         try:
#             print("Fetching data...")
#             logging.info("Fetching data...")
#             df = fetch_data_from_databricks(query, connection)
#             print(f"Fetched {len(df)} rows from Databricks")
#             logging.info(f"Fetched {len(df)} rows from Databricks")
#         finally:
#             connection.close()
#             print("Databricks connection closed")
#             logging.info("Databricks connection closed")
        
#         print("=== STARTING FILE OPERATIONS ===")
#         logging.info("=== STARTING FILE OPERATIONS ===")
        
#         # Generate file
#         current_date = get_current_date()
#         blob_name = f'grapplerau_delta_{current_date}.csv'
#         df['file_name'] = blob_name
        
#         # Save to temporary location
#         temp_file_path = f'/tmp/grapplerau_delta_{current_date}.csv'
#         df.to_csv(temp_file_path, index=False)
#         print(f"Saved CSV to {temp_file_path}")
#         logging.info(f"Saved CSV to {temp_file_path}")
        
#         print("=== STARTING SFTP UPLOAD ===")
#         logging.info("=== STARTING SFTP UPLOAD ===")
        
#         # SFTP upload
#         ssh = paramiko.SSHClient()
#         ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
#         private_key = paramiko.RSAKey.from_private_key(io.StringIO(os.getenv("OPENSSH_KEY")))
#         ssh.connect(SFTP_HOST, username=SFTP_USERNAME, pkey=private_key)
        
#         sftp = ssh.open_sftp()
#         sftp.put(temp_file_path, f'grapplerau_delta_{current_date}.csv')
#         sftp.close()
#         ssh.close()
        
#         # Clean up
#         os.remove(temp_file_path)
        
#         print(f"Successfully uploaded {blob_name} to SFTP")
#         logging.info(f"Successfully uploaded {blob_name} to SFTP")
        
#     except Exception as e:
#         print(f"Function execution failed: {str(e)}")
#         logging.error(f"Function execution failed: {str(e)}", exc_info=True)
#         raise
        
#     print("=== FUNCTION COMPLETED ===")
#     logging.info("=== FUNCTION COMPLETED ===")


import azure.functions as func
import logging
import sys

def main(mytimer: func.TimerRequest) -> None:
    # Configure logging to ensure output appears
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    print("=== DEBUG: Function execution started ===")
    logging.info("=== DEBUG: Function execution started ===")
    
    try:
        print("=== Testing imports one by one ===")
        logging.info("=== Testing imports one by one ===")
        
        import os
        print("✓ os import successful")
        logging.info("✓ os import successful")
        
        from dotenv import load_dotenv
        print("✓ dotenv import successful")
        logging.info("✓ dotenv import successful")
        
        import pandas as pd
        print("✓ pandas import successful")
        logging.info("✓ pandas import successful")
        
        from databricks import sql
        print("✓ databricks import successful")
        logging.info("✓ databricks import successful")
        
        import paramiko
        print("✓ paramiko import successful")
        logging.info("✓ paramiko import successful")
        
        print("=== All imports successful! ===")
        logging.info("=== All imports successful! ===")
        
        # Test environment variables
        load_dotenv()
        
        databricks_host = os.getenv('DATABRICKS_SERVER_HOSTNAME')
        print(f"DATABRICKS_HOST found: {databricks_host is not None}")
        logging.info(f"DATABRICKS_HOST found: {databricks_host is not None}")
        
        if databricks_host is None:
            raise ValueError("DATABRICKS_SERVER_HOSTNAME environment variable not found")
        
        print("=== Function completed successfully ===")
        logging.info("=== Function completed successfully ===")
        
    except ImportError as e:
        error_msg = f"Import Error: {str(e)}"
        print(error_msg)
        logging.error(error_msg)
        raise
    except Exception as e:
        error_msg = f"General Error: {str(e)}"
        print(error_msg)
        logging.error(error_msg)
        raise