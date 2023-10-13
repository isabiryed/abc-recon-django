import logging
import math
import re
import pyodbc
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime

# Configuring logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def execute_query(server_name, database_name, username, password, query, query_type="SELECT"):
    conn = None  # Initialize conn to None
    try:
        # Define the connection string
        connection_string = f"Driver={{SQL Server}};Server={server_name};Database={database_name};UID={username};PWD={password};TrustServerCertificate=yes;"

        # Connect to the database
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Execute the query based on the query type
        if query_type == "SELECT":
            cursor.execute(query)
            rows = cursor.fetchall()
            if not rows:
                return pd.DataFrame()
            return pd.DataFrame.from_records(rows, columns=[column[0] for column in cursor.description])
            
        elif query_type in ["UPDATE", "INSERT"]:
            cursor.execute(query)
            conn.commit()  # Commit the changes to the database
            return None  # For update and Insert queries, return None
        else:
            raise ValueError("Invalid query type. Supported types are 'SELECT','UPDATE'and 'INSERT'.")

    except Exception as e:
        print(f"Error: {str(e)}")
        return None  # Return None in case of an error
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Example usage for SELECT query:    

    # Load the .env file
    load_dotenv()

    # Get the environment variables
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_NAME')
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')
    # Test the connection
    test_query = "SELECT TOP 2 DATE_TIME, TRN_REF, TXN_TYPE FROM Transactions"  # A simple query to test the connection
    result_df = execute_query(server, database, username, password, test_query)

    if result_df is not None:
        print("Connection successful.")
        print(result_df)  # Print the result of the test query
    else:
        print("Connection failed.")

def select_exceptions(server, database, username, password,Swift_code_up):
    # Define the SQL query for selection
    excep_select_query = f"""
        SELECT DISTINCT DATE_TIME, TRAN_DATE, TRN_REF, BATCH, ACQUIRER_CODE, ISSUER_CODE, EXCEP_FLAG,
            CASE WHEN ACQ_FLG = 1 OR ISS_FLG =1 THEN 'Partly Receonciled' WHEN ACQ_FLG = 1 AND ISS_FLG =1 THEN 'Fully Reconciled' END AS RECON_STATUS

            FROM reconciliation 
            WHERE EXCEP_FLAG IS NOT NULL
            AND (ISSUER_CODE = '{Swift_code_up}' OR ACQUIRER_CODE = '{Swift_code_up}')  """
    
    # Execute the SQL query and retrieve the results
    excep_results = execute_query(server, database, username, password, excep_select_query, query_type="SELECT")
    
    return excep_results

def update_reconciliation(df, server, database, username, password, swift_code):

    # Check if necessary columns exist in the DataFrame
    required_columns = ['DATE_TIME', 'BATCH', 'TRN_REF2', 'ISSUER_CODE', 'ACQUIRER_CODE']
    for col in required_columns:
        if col not in df.columns:
            logging.error(f"The '{col}' column is missing from the DataFrame.")
            return f"The '{col}' column is missing from the DataFrame."

    if df.empty:
        logging.warning("No Records to Update.")
        return

    update_count = 0
    insert_count = 0

    for index, row in df.iterrows():
        date_time = row['DATE_TIME']
        batch = row['BATCH']
        trn_ref = row['TRN_REF2']
        issuer_code = row['ISSUER_CODE']
        acquirer_code = row['ACQUIRER_CODE']

        if pd.isnull(trn_ref):
            logging.warning(f"Empty Trn Reference for {index}.")
            continue

        select_query = f"SELECT * FROM reconciliation WHERE TRN_REF = '{trn_ref}'"
        existing_data = execute_query(server, database, username, password, select_query, query_type="SELECT")
        if existing_data is None:
            logging.error(f"Failed to fetch data for TRN_REF '{trn_ref}'.")
            continue

        # Update Query
        update_query = f"""
            UPDATE reconciliation
        SET
            ISS_FLG = CASE WHEN (ISS_FLG IS NULL OR ISS_FLG = 0 OR ISS_FLG != 1)  AND ISSUER_CODE = '{swift_code}' THEN 1 ELSE ISS_FLG END,
            ACQ_FLG = CASE WHEN (ACQ_FLG IS NULL OR ACQ_FLG = 0 OR ACQ_FLG != 1) AND ACQUIRER_CODE = '{swift_code}' THEN 1 ELSE ACQ_FLG END,
            ISS_FLG_DATE = CASE WHEN (ISS_FLG IS NULL OR ISS_FLG = 0 OR ISS_FLG != 1) AND ISSUER_CODE = '{swift_code}' THEN GETDATE() ELSE ISS_FLG_DATE END,
            ACQ_FLG_DATE = CASE WHEN (ACQ_FLG IS NULL OR ACQ_FLG = 0 OR ACQ_FLG != 1) AND ACQUIRER_CODE = '{swift_code}' THEN GETDATE() ELSE ACQ_FLG_DATE END
            WHERE TRN_REF = '{trn_ref}'                
        """

        if existing_data.empty:
            # If not existing, insert and then update
            insert_query = f"""
                INSERT INTO reconciliation 
                    (DATE_TIME, TRAN_DATE, TRN_REF, BATCH, ACQUIRER_CODE, ISSUER_CODE)
                VALUES 
                    (GETDATE(),
                     '{date_time}',
                     '{trn_ref}', 
                     '{batch}', 
                     '{acquirer_code}',
                     '{issuer_code}')
            """
            try:
                execute_query(server, database, username, password, insert_query, query_type="INSERT")
                insert_count += 1
                # Immediate update after insert
                execute_query(server, database, username, password, update_query, query_type="UPDATE")
                # update_count += 1
            except pyodbc.Error as err:
                logging.error(f"Error processing PK '{trn_ref}': {err}")
        else:
            # If already existing, just update
            try:
                execute_query(server, database, username, password, update_query, query_type="UPDATE")
                update_count += 1
            except pyodbc.Error as err:
                logging.error(f"Error updating PK '{trn_ref}': {err}")

    if update_count == 0:
        logging.info("No new records were updated.")
    if insert_count == 0:
        logging.info("No new records were inserted.")

    feedback = f"Updated: {update_count}, Inserted: {insert_count}"
    logging.info(feedback)

    return feedback

def insert_recon_stats(bankid,userid,reconciledRows, unreconciledRows, exceptionsRows, feedback, 
                       requestedRows, UploadedRows, date_range_str, server, database, username, password):
    # Define the SQL query for insertion
    insert_query = f"""
        INSERT INTO reconciliationLogs
        (DATE_TIME,BANK_ID, USER_ID,RECON_RWS, UNRECON_RWS, EXCEP_RWS, FEEDBACK, RQ_RWS, UPLD_RWS, RQ_DATE_RANGE)
        VALUES
        ('{current_datetime}',{bankid},{userid},{reconciledRows}, {unreconciledRows}, {exceptionsRows}, '{feedback}', {requestedRows}, {UploadedRows}, '{date_range_str}')
    """
    
    # Execute the SQL query
    execute_query(server, database, username, password, insert_query, query_type = "INSERT")

def recon_stats_req(server, database, username, password, bank_id):
    # Define the SQL query for selection using an f-string to insert swift_code
    select_query = f"""
        SELECT RQ_RWS, RQ_DATE_RANGE, UPLD_RWS, EXCEP_RWS, RECON_RWS, UNRECON_RWS, FEEDBACK 
        FROM reconciliationLogs WHERE BANK_ID = '{bank_id}'
    """
    
    # Execute the SQL query and retrieve the results
    recon_results = execute_query(server, database, username, password, select_query, query_type="SELECT")
    
    return recon_results

def select_reversals(server, database, username, password, swift_code_up):
    # SQL query to select distinct reversals
    reversals_select_query = f"""
        SELECT DISTINCT
            A.DATE_TIME, A.TRN_REF, A.TXN_TYPE, A.ISSUER, A.ACQUIRER, A.AMOUNT,
            A.REQUEST_TYPE AS FIRST_REQUEST,
            A.AGENT_CODE,
            CASE WHEN A.TRAN_STATUS_0 IN ('0') THEN 'Successful' ELSE 'Failed' END AS FIRST_LEG_RESP,
            CASE WHEN A.TRAN_STATUS_1 IN ('0') THEN 'Successful' ELSE 'Failed' END AS SECND_LEG_RESP,
            CASE WHEN B.RESPONSE_CODE IN ('0') THEN 'Successful' ELSE 'Failed' END AS REV_STATUS,
            CASE WHEN B.RESPONSE_CODE IN ('0') THEN NULL ELSE DATEDIFF(SECOND, A.DATE_TIME, GETDATE()) END AS ELAPSED_TIME,
            CASE 
                WHEN B.REQUEST_TYPE IN ('1420') THEN 'First Reversal'
                WHEN B.REQUEST_TYPE IN ('1421') THEN 'Repeat Reversal'
                ELSE 'Unknown' 
            END AS REVERSAL_TYPE	   
    
        FROM Transactions A
        LEFT JOIN (
            SELECT REQUEST_TYPE, TRAN_REF_1, TRAN_REF_0, TRAN_STATUS_1, RESPONSE_CODE
            FROM Transactions
            WHERE REQUEST_TYPE IN ('1420', '1421')
        ) B ON A.TRAN_REF_0 = B.TRAN_REF_1
        WHERE 
            A.REQUEST_TYPE IN ('1200') 
            AND (A.AMOUNT <> 0)
            AND A.TRAN_STATUS_1 IS NOT NULL 
            AND A.TRAN_STATUS_0 IS NOT NULL
            AND (
                (A.TRAN_STATUS_0 IN ('0','00') AND A.TRAN_STATUS_1 NOT IN ('null','00','0')) 
                OR 
                (A.TRAN_STATUS_1 IN ('0','00') AND A.TRAN_STATUS_0 NOT IN ('null','00','0'))
            )
            AND A.TXN_TYPE NOT IN ('ACI','AGENTFLOATINQ','BI','MINI')
            AND (ISSUER_CODE = '{swift_code_up}' OR ACQUIRER_CODE = '{swift_code_up}')
        GROUP BY
            A.DATE_TIME, A.TRN_REF, A.TXN_TYPE, A.ISSUER, A.ACQUIRER, A.AMOUNT,
            A.REQUEST_TYPE, B.REQUEST_TYPE, A.TRAN_REF_0, A.TRAN_REF_1,
            A.AGENT_CODE, B.RESPONSE_CODE, A.RESPONSE_CODE, A.TRAN_STATUS_0, A.TRAN_STATUS_1
        ORDER BY A.DATE_TIME, A.TRN_REF DESC;
    """
    
    # Execute the SQL query and retrieve the results
    reversal_results = execute_query(server, database, username, password, reversals_select_query, query_type="SELECT")
    
    return reversal_results

def select_setle_file(server, database, username, password, batch):
    try:
        # Define the SQL query for selection
        query = f"""
                SELECT DATE_TIME,TRN_REF, BATCH, TXN_TYPE, ISSUER, ACQUIRER, AMOUNT, FEE, ABC_COMMISSION
                FROM Transactions 
                WHERE (RESPONSE_CODE = '0') 
                AND BATCH = '{batch}' 
                AND ISSUER_CODE != '730147'
                AND TXN_TYPE NOT IN ('ACI', 'AGENTFLOATINQ')
                AND REQUEST_TYPE NOT IN ('1420','1421')
                """
        
        # Execute the SQL query and retrieve the results
        cursor = execute_query(server, database, username, password, query)
        
        # If the cursor is None, handle the error
        if cursor is None:
            raise Exception("No cursor returned from query execution.")

        # datafile = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        datafile = pd.DataFrame(cursor)

        return datafile
    except Exception as e:
        logging.error(f"Error fetching data from the database: {str(e)}")
        return None  
    
def update_exception_flag(df, server, database, username, password, swift_code):

    # Check if 'trn_ref' column exists in the DataFrame
    if 'TRN_REF2' not in df.columns:
        logging.error("The 'trn_ref' column is missing from the DataFrame.")
        return "The 'trn_ref' column is missing from the DataFrame."

    # If the dataframe is empty, just return the appropriate message
    if len(df) < 1:
        return "No Exceptions Found."

    update_count = 0

    for _, row in df.iterrows():
        trn_ref = row['TRN_REF2']

        if pd.isnull(trn_ref):
            continue  # Skip the current iteration if trn_ref is null

        # Update Query
        update_query = f"""
            UPDATE reconciliation
        SET
            EXCEP_FLAG = CASE WHEN (EXCEP_FLAG IS NULL OR EXCEP_FLAG = 0 OR EXCEP_FLAG != 1)  
            AND (ISSUER_CODE = '{swift_code}' OR ACQUIRER_CODE = '{swift_code}')  
            THEN 'Y' ELSE 'N' END            
            WHERE TRN_REF = '{trn_ref}'
        """

        try:
            execute_query(server, database, username, password, update_query, query_type="UPDATE")
            update_count += 1
        except pyodbc.Error as err:
            # If there's an error, log it and continue to the next iteration
            logging.error(f"Error updating PK '{trn_ref}': {err}")
            continue

    # If no updates were made, return the appropriate message
    if update_count == 0:
        return "No Exceptions were updated."

    return f"Exceptions updated: {update_count}"


def use_cols(df):
    """
    Renames the 'Original_ABC Reference' column to 'Reference' and selects specific columns.

    :param df: DataFrame to be processed.
    :return: New DataFrame with selected and renamed columns.
    """
    df = df.rename(columns={'TXN_TYPE_y': 'TXN_TYPE', 'Original_TRN_REF': 'TRN_REF2'})

    # Convert 'DATE_TIME' to datetime
    df['DATE_TIME'] = pd.to_datetime(df['DATE_TIME'].astype(str), format='%Y%m%d')

    # Select only the desired columns
    selected_columns = ['DATE_TIME', 'AMOUNT', 'TRN_REF2', 'BATCH', 'TXN_TYPE', 
                        'ISSUER_CODE', 'ACQUIRER_CODE', 'RESPONSE_CODE', '_merge', 'Recon Status']
    df_selected = df[selected_columns]
    
    return df_selected

def backup_refs(df, reference_column):
    # Backup the original reference column
    df['Original_' + reference_column] = df[reference_column]
    
    return df

def date_range(dataframe, date_column):
    min_date = dataframe[date_column].min().strftime('%Y-%m-%d')
    max_date = dataframe[date_column].max().strftime('%Y-%m-%d')
    return min_date, max_date

def process_reconciliation(DF1: pd.DataFrame, DF2: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame):
    
    # Rename columns of DF1 to match DF2 for easier merging
    DF1 = DF1.rename(columns={'Date': 'DATE_TIME','ABC Reference': 'TRN_REF','Amount': 'AMOUNT','Transaction type': 'TXN_TYPE'})
    
    # Merge the dataframes on the relevant columns
    merged_df = DF1.merge(DF2, on=['DATE_TIME', 'TRN_REF', 'AMOUNT'], how='outer', indicator=True)
    
    # Create a new column 'Recon Status'
    merged_df['Recon Status'] = 'Unreconciled'
    merged_df.loc[(merged_df['Recon Status'] == 'Unreconciled') & (merged_df['RESPONSE_CODE'] == '0') | (merged_df['Response_code'] == '0'), 'Recon Status'] = 'succunreconciled'
    merged_df.loc[merged_df['_merge'] == 'both', 'Recon Status'] = 'Reconciled'

    # Separate the data into three different dataframes based on the reconciliation status
    reconciled_data = merged_df[merged_df['Recon Status'] == 'Reconciled']
    succunreconciled_data = merged_df[merged_df['Recon Status'] == 'succunreconciled']
    exceptions = merged_df[(merged_df['Recon Status'] == 'Reconciled') & (merged_df['RESPONSE_CODE'] != '0')]

    return merged_df, reconciled_data, succunreconciled_data, exceptions

def unserializable_floats(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace({math.nan: "NaN", math.inf: "Infinity", -math.inf: "-Infinity"})
    return df

def pre_processing(df):
    # Helper functions
    def clean_amount(value):
        try:
            # Convert the value to a float and then to an integer to remove decimals
            return str(int(float(value)))
        except:
            return '0'  # Default to '0' if conversion fails
    
    def remo_spec_x(value):
        cleaned_value = re.sub(r'[^0-9a-zA-Z]', '', str(value))
        if cleaned_value == '':
            return '0'
        return cleaned_value
    
    def pad_strings_with_zeros(input_str):
        if len(input_str) < 12:
            num_zeros = 12 - len(input_str)
            padded_str = '0' * num_zeros + input_str
            return padded_str
        else:
            return input_str[:12]

    def clean_date(value):
        try:
            # Convert to datetime to ensure it's in datetime format
            date_value = pd.to_datetime(value).date()
            return str(date_value).replace("-", "")
        except:
            return value  # Return the original value if conversion fails

    # Cleaning logic
    for column in df.columns:
        # Cleaning for date columns
        if column in ['Date', 'DATE_TIME']:
            df[column] = df[column].apply(clean_date)
        # Cleaning for amount columns
        elif column in ['Amount', 'AMOUNT']:
            df[column] = df[column].apply(clean_amount)
        else:
            df[column] = df[column].apply(remo_spec_x)  # Clean without converting to string
        
        # Padding for specific columns
        if column in ['ABC Reference', 'TRN_REF']:
            df[column] = df[column].apply(pad_strings_with_zeros)
    
    return df    
    
def pre_processing_amt(df):
    # Helper function
    def clean_amount(value):
        try:
            # Convert the value to a float, round to nearest integer
            return round(float(value))  # round the value and return as integer
        except:
            return value  # Return the original value if conversion fails
    
    # Cleaning logic
    for column in ['AMOUNT', 'FEE', 'ABC_COMMISSION']:  # only these columns
        df[column] = df[column].apply(clean_amount)
    
    return df

def read_excel_file(file_path, sheet_name):
    try:
        with pd.ExcelFile(file_path) as xlsx:
            df = pd.read_excel(xlsx, sheet_name=sheet_name, usecols=[0, 1, 2, 7, 8, 9, 11], skiprows=0)
        # Rename the columns
        df.columns = ['TRN_REF', 'DATE_TIME', 'BATCH', 'TXN_TYPE', 'AMOUNT', 'FEE', 'ABC_COMMISSION']
        return df
    except Exception as e:
        logging.error(f"An error occurred while opening the Excel file: {e}")
        return None

def setlement_process_recon(DF1: pd.DataFrame, DF2: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
    
    # Merge the dataframes on the relevant columns
    merged_setle = DF1.merge(DF2, on=['DATE_TIME', 'TRN_REF'], how='outer', suffixes=('_DF1', '_DF2'), indicator=True)
    
        # Now perform the subtraction
    merged_setle.loc[merged_setle['_merge'] == 'both', 'AMOUNT_DIFF'] = (
        pd.to_numeric(merged_setle['AMOUNT_DF1'], errors='coerce') - 
        pd.to_numeric(merged_setle['AMOUNT_DF2'], errors='coerce')
    )

    merged_setle.loc[merged_setle['_merge'] == 'both', 'ABC_COMMISSION_DIFF'] = (
        pd.to_numeric(merged_setle['ABC_COMMISSION_DF1'], errors='coerce') - 
        pd.to_numeric(merged_setle['ABC_COMMISSION_DF2'], errors='coerce')
    )
    
    # Create a new column 'Recon Status'
    merged_setle['Recon Status'] = 'Unreconciled'    
    merged_setle.loc[merged_setle['_merge'] == 'both', 'Recon Status'] = 'Reconciled'
    
    # Separate the data into different dataframes based on the reconciliation status
    matched_setle = merged_setle[merged_setle['Recon Status'] == 'Reconciled']
    unmatched_setle = merged_setle[merged_setle['Recon Status'] == 'Unreconciled']
    unmatched_setlesabs = merged_setle[(merged_setle['AMOUNT_DIFF'] != 0) | (merged_setle['ABC_COMMISSION_DIFF'] != 0)]
    
    # Define the columns to keep for merged_setle
    use_columns = ['TRN_REF', 'DATE_TIME', 'BATCH_DF1', 'TXN_TYPE_DF1', 'AMOUNT_DF1', 
                            'FEE_DF1', 'ABC_COMMISSION_DF1', 'AMOUNT_DIFF', 'ABC_COMMISSION_DIFF', 
                            '_merge', 'Recon Status']

    # Select only the specified columns for merged_setle
    merged_setle = merged_setle.loc[:, use_columns]    
    matched_setle = matched_setle.loc[:, use_columns]
    unmatched_setle = unmatched_setle.loc[:, use_columns]
    unmatched_setlesabs = unmatched_setlesabs.loc[:, use_columns]

    return merged_setle, matched_setle, unmatched_setle,unmatched_setlesabs

def combine_transactions(df: pd.DataFrame, acquirer_col: str = 'Payer', issuer_col: str = 'Beneficiary', 
                         amount_col: str = 'Tran Amount', type_col: str = 'Tran Type') -> pd.DataFrame:
    """
    Combine transactions based on certain conditions.

    :param df: Input DataFrame.
    :param acquirer_col: Column name for Acquirer.
    :param issuer_col: Column name for Issuer.
    :param amount_col: Column name for Transaction Amount.
    :param type_col: Column name for Transaction Type.
    :return: New DataFrame with combined transaction amounts.
    """
    combined_dict = {}

    for index, row in df.iterrows():
        acquirer = row[acquirer_col]
        issuer = row[issuer_col]
        tran_amount = row[amount_col]
        tran_type = row[type_col]
        key = (acquirer, issuer)
    
        if acquirer != issuer and tran_type not in ["CLF", "CWD"]:
            combined_dict[key] = combined_dict.get(key, 0) + tran_amount

        if acquirer != issuer and tran_type in ["CLF", "CWD"]:
            combined_dict[key] = combined_dict.get(key, 0) + tran_amount

        # where issuer & acquirer = TROP BANK AND service = NWSC , UMEME settle them with BOA
        if acquirer == "TROAUGKA" and issuer == "TROAUGKA" and tran_type in ["NWSC", "UMEME"]:
            tro_key = ("TROAUGKA", "AFRIUGKA")
            combined_dict[tro_key] = combined_dict.get(tro_key, 0) + tran_amount

    # Convert combined_dict to DataFrame
    combined_result = pd.DataFrame(combined_dict.items(), columns=["Key", amount_col])
    # Split the "Key" column into Acquirer and Issuer columns
    combined_result[[acquirer_col, issuer_col]] = pd.DataFrame(combined_result["Key"].tolist(), index=combined_result.index)
    
    # Drop the "Key" column
    combined_result = combined_result.drop(columns=["Key"])
    
    return combined_result

def add_payer_beneficiary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds 'Payer' and 'Beneficiary' columns to the DataFrame.

    :param df: Input DataFrame.
    :return: DataFrame with 'Payer' and 'Beneficiary' columns added.
    """
    df['Payer'] = df['ACQUIRER']
    df['Beneficiary'] = df['ISSUER']
    return df

def convert_batch_to_int(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts the 'BATCH' column to numeric, rounds it to the nearest integer, and fills NaN with 0.

    :param df: DataFrame containing the 'BATCH' column to convert.
    :return: DataFrame with the 'BATCH' column converted.
    """
    # Check data type and convert 'BATCH' column to numeric
    df['BATCH'] = pd.to_numeric(df['BATCH'], errors='coerce')
    # Apply the round method
    df['BATCH'] = df['BATCH'].round(0).fillna(0).astype(int)
    
    return df