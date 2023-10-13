from django.http import JsonResponse
from django.db.models import Q
import os

from recon.utils import backup_refs, date_range, insert_recon_stats, pre_processing, process_reconciliation, update_exception_flag, update_reconciliation
from .models import Transactions  # Assuming you have a model named 'Transactions'
import pandas as pd

from dotenv import load_dotenv

# Load the .env file
load_dotenv()

# Get the environment variables
server = os.getenv('DB_SERVER')
database = os.getenv('DB_NAME')
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')

def reconcileMain(request):
    
    # Ensure the request method is POST and contains necessary data
    if request.method != 'POST' or 'file' not in request.FILES or 'Swift_code_up' not in request.POST:
        return JsonResponse({'error': 'Invalid request!'})

    uploaded_file = request.FILES['file']
    Swift_code_up = request.POST['Swift_code_up']  # Grabbing Swift_code_up from POST data

    # Handle File Uploads
    uploaded_file = request.FILES['file']
    uploaded_df = pd.read_excel(uploaded_file, usecols=[0, 1, 2, 3], skiprows=0)

    # Get the date range from uploaded file
    min_date, max_date = date_range(uploaded_df, 'Date')
    date_range_str = f"{min_date},{max_date}"

    # Keep a copy of the refs for future us before data manipulation
    uploaded_df = backup_refs(uploaded_df, 'ABC Reference')
    uploaded_df['Response_code'] = '0'
    UploadedRows = len(uploaded_df)
    
    # Clean and format columns in the uploaded dataset
    uploaded_df_processed = pre_processing(uploaded_df)        

    # Use Django ORM for database interactions
    datadump = Transactions.objects.filter(
        Q(ISSUER_CODE=Swift_code_up) | Q(ACQUIRER_CODE=Swift_code_up),
        DATE_TIME__date__range=(min_date, max_date),
        REQUEST_TYPE__in=['1420', '1421'],
        AMOUNT__ne=0,
        TXN_TYPE__in=['ACI', 'AGENTFLOATINQ', 'BI', 'MINI']
    ).values()  # Convert the queryset to a list of dictionaries

    datadump_df = pd.DataFrame.from_records(datadump)  # Convert the list of dictionaries to a DataFrame

    # Keep a copy of the refs for future us before data manipulation
    datadump_df = backup_refs(datadump_df, 'TRN_REF')
    req_file = datadump_df[datadump_df['RESPONSE_CODE'] == '0']
    requestedRows = len(req_file)

    # Clean and format columns in the datadump        
    db_preprocessed = pre_processing(datadump_df)

    # The actual reconciliation happens here       
    merged_df, reconciled_data, succunreconciled_data, exceptions = process_reconciliation(uploaded_df_processed, db_preprocessed)  
    
    # Update / insert into reconciliation table reconciled rows 
    feedback = update_reconciliation(reconciled_data, server, database, username, password, Swift_code_up)      

    # Initialize exceptions_feedback with a default value
    exceptions_feedback = "No exceptions to update." 
    # Check if exceptions is not None and if it's not empty
    if exceptions is not None and len(exceptions) > 0:
        exceptions_feedback = update_exception_flag(exceptions, server, database, username, password,Swift_code_up)
            
    insert_recon_stats(
        Swift_code_up, Swift_code_up, len(reconciled_data), len(succunreconciled_data),
        len(exceptions), feedback, (requestedRows), (UploadedRows), 
        date_range_str, server, database, username, password ) 

    return JsonResponse({
        'merged_df': merged_df.to_dict(),
        'reconciled_data': reconciled_data.to_dict(),
        'succunreconciled_data': succunreconciled_data.to_dict(),
        'exceptions': exceptions.to_dict(),
        'feedback': feedback,
        'requestedRows': requestedRows,
        'UploadedRows': UploadedRows,
        'date_range_str': date_range_str
    })
