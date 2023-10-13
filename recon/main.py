import glob
import pandas as pd
from recon.utils import add_payer_beneficiary, combine_transactions, convert_batch_to_int, pre_processing,backup_refs, date_range, execute_query, insert_recon_stats, pre_processing_amt, process_reconciliation, read_excel_file, select_setle_file, setlement_process_recon, update_exception_flag, update_reconciliation, use_cols
import os
import logging

reconciled_data = None
succunreconciled_data = None
batch = 2349

from dotenv import load_dotenv

# Load the .env file
load_dotenv()

# Get the environment variables
server = os.getenv('DB_SERVER')
database = os.getenv('DB_NAME')
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')

def reconcileMain(request):
    try:
        global reconciled_data, succunreconciled_data  # Indicate these are global variables
        
        # Read the uploaded dataset from Excel
        uploaded_file = request.FILES['file']  # Assuming 'file' is the name attribute in your HTML form
        uploaded_df = pd.read_excel(uploaded_file, usecols=[0, 1, 2, 3], skiprows=0)

        # Now, get the date range from uploaded file
        min_date, max_date = date_range(uploaded_df, 'Date')
        date_range_str = f"{min_date},{max_date}"

        # Keep a copy of the refs for future us before data manipulation
        uploaded_df = backup_refs(uploaded_df, 'ABC Reference')
        uploaded_df['Response_code'] = '0'
        UploadedRows = len(uploaded_df)
        
        # Simple Clean and format columns in the uploaded dataset
        uploaded_df_processed = pre_processing(uploaded_df)        
        
        query = f"""
         SELECT DISTINCT DATE_TIME, BATCH,TRN_REF, TXN_TYPE, ISSUER_CODE, ACQUIRER_CODE,
                AMOUNT, RESPONSE_CODE
         FROM Transactions
         WHERE (ISSUER_CODE = '{Swift_code_up}' OR ACQUIRER_CODE = '{Swift_code_up}')
             AND CONVERT(DATE, DATE_TIME) BETWEEN '{min_date}' AND '{max_date}'
             AND REQUEST_TYPE NOT IN ('1420','1421')
            AND AMOUNT <> 0
            AND TXN_TYPE NOT IN ('ACI','AGENTFLOATINQ','BI','MINI')
     """
        # Execute the SQL query
        datadump = execute_query(server, database, username, password, query, query_type="SELECT")
        
        if datadump is not None:
            # Keep a copy of the refs for future us before data manipulation
            datadump = backup_refs(datadump, 'TRN_REF')
            req_file = datadump[datadump['RESPONSE_CODE'] == '0']
            requestedRows = len(req_file)

            # Clean and format columns in the datadump        
            db_preprocessed = pre_processing(datadump)

            # The actual reconciliation happens here       
            merged_df, reconciled_data, succunreconciled_data, exceptions = process_reconciliation(uploaded_df_processed, db_preprocessed)  
            print(reconciled_data.head(3))
            print(succunreconciled_data.head(3))

            # Update / insert into reconciliation table reconciled rows 
            feedback = update_reconciliation(reconciled_data, server, database, username, password, Swift_code_up)      
            
            # Update the exceptions Flag, if we have exceptions in the reconciled
            #Initialize exceptions_feedback with a default value
            exceptions_feedback = "No exceptions to update." 
            # Check if exceptions is None
            if len(exceptions) < 1:
                logging.error("Exceptions is None!")

            # Check if exceptions is not None and if it's not empty
            elif len(exceptions) > 0:
                exceptions_feedback = update_exception_flag(exceptions, server, database, username, password,Swift_code_up)
                
            insert_recon_stats(
                Swift_code_up, Swift_code_up, len(reconciled_data), len(succunreconciled_data),
                len(exceptions), feedback, (requestedRows), (UploadedRows), 
                date_range_str, server, database, username, password ) 
                
            print('Reconciliation is complete.' + feedback)
            return merged_df, reconciled_data, succunreconciled_data, exceptions, feedback, requestedRows, UploadedRows, date_range_str

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return None, None, None, None, None, None, None, None    

def settlement_main(batch):

    try:

        logging.basicConfig(filename='settlement.log', level=logging.ERROR)

        # Execute the SQL query
        datadump = select_setle_file(server, database, username, password, batch)
        
        # Check if datadump is not None
        if datadump is not None and not datadump.empty:         
            datadump = convert_batch_to_int(datadump)
            datadump = pre_processing_amt(datadump)
            datadump = add_payer_beneficiary(datadump)            
                  
        else:
            logging.warning("No records for processing found.")
            return None  # Return None to indicate that no records were found

        setlement_result = combine_transactions(datadump, acquirer_col='Payer', issuer_col='Beneficiary', amount_col='AMOUNT', type_col='TXN_TYPE')

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return None  # Return None to indicate that an error occurred

    return setlement_result 

def setlement_recon_main(path,batch): 

    try:
     
        datadump = select_setle_file(server, database, username, password, batch)
        
        # Check if datadump is not None and not empty
        if datadump is not None and not datadump.empty:         
            datadump = pre_processing_amt(datadump)
            datadump = pre_processing(datadump)
            
        else:
            print("No records for processing found.")

        # Processing SABSfile_ regardless of datadump's status
        excel_files = glob.glob(path)
        if not excel_files:
            logging.error(f"No matching Excel file found for '{path}'.")
        else:
            matching_file = excel_files[0]
            SABSfile_ = read_excel_file(matching_file, 'Transaction Report')
            SABSfile_ = pre_processing_amt(SABSfile_)
            SABSfile_ = pre_processing(SABSfile_)           
        
        merged_setle, matched_setle,unmatched_setle,unmatched_setlesabs = setlement_process_recon(SABSfile_,datadump)
        # print(unmatched_setlesabs.head(10))

        logging.basicConfig(filename = 'settlement_recon.log', level = logging.ERROR)
            
        print('Thank you, your settlement Report is ready')
        # pass
    except Exception as e:
        logging.error(f"Error: {str(e)}")

    return merged_setle,matched_setle,unmatched_setle,unmatched_setlesabs

