from io import BytesIO
from zipfile import ZipFile
from django.http import FileResponse, Http404
from django.shortcuts import render
from rest_framework import generics,status
from recon.db_exceptions import select_exceptions
from recon.setle_sabs import setleSabs
from recon.setlement_ import settle

from .db_reversals import select_reversals

from .mainFile import reconcileMain, unserializable_floats

from .db_recon_stats import recon_stats_req

from .models import Reconciliation,ReconciliationLog
from .serializers import ReconcileSerializer, ReconciliationSerializer, SabsSerializer, SettlementSerializer
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import AuthenticationFailed
# from rest_framework.permissions import IsAuthenticated  # Import this for adding permissions

# thisi can bechanged
from dotenv import load_dotenv
import os
# Load the .env file
load_dotenv()

# Get the environment variables
server = os.getenv('DB_SERVER')
database = os.getenv('DB_NAME')
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')

class ReconciliationListView(generics.ListCreateAPIView):
    queryset = Reconciliation.objects.all()
    serializer_class = ReconciliationSerializer

class ReconciliationLogListView(generics.ListAPIView):
    queryset = ReconciliationLog.objects.all()
    
class ReconcileView(APIView):
    serializer_class = ReconcileSerializer
    # permission_classes = [IsAuthenticated]


    def post(self, request):
        serializer = self.serializer_class(data=request.data)
        if serializer.is_valid():
            uploaded_file = serializer.validated_data['file']
            swift_code = serializer.validated_data['swift_code']

            # Save the uploaded file temporarily
            temp_file_path = "temp_file.xlsx"
            with open(temp_file_path, "wb") as buffer:
                buffer.write(uploaded_file.read())

            try:
                # Call the main function with the path of the saved file and the swift code
                merged_df, reconciled_data, succunreconciled_data, exceptions, feedback, requestedRows, UploadedRows, date_range_str = reconcileMain(
                    temp_file_path, swift_code)
                
                # Perform clean up: remove the temporary file after processing
                os.remove(temp_file_path)
                
                data = {
                    "reconciledRows": len(reconciled_data),
                    "unreconciledRows": len(succunreconciled_data),
                    "exceptionsRows": len(exceptions),
                    "feedback": feedback,
                    "RequestedRows": requestedRows,
                    "UploadedRows": UploadedRows,
                    "min_max_DateRange": date_range_str
                }

                return Response(data, status=status.HTTP_200_OK)

            except Exception as e:
                # If there's an error during the process, ensure the temp file is removed
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)
                
                # Return error as response
                return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
class ReversalsView(APIView):
    """
    Retrieve reversal data.
    """
   
    def get(self, request, *args, **kwargs):
        swift_code_up = request.query_params.get('swift_code_up')
       
        if not swift_code_up:
            return Response({"error": "swift_code_up parameter is required"}, status=status.HTTP_400_BAD_REQUEST)

        # Use your database function to get the reversal data
        data = select_reversals(
        server,
        database,
        username,
        password,
        swift_code_up
        )

        # The data returned by select_reversals is assumed to be in a suitable format for JSON serialization

        return Response(data, status=status.HTTP_200_OK)
class ReversalsView(APIView):
    """
    Retrieve reversal data.
    """

    def get(self, request, swift_code_up, *args, **kwargs):
        # Use values from .env for database connection
        data = select_reversals(server, database, username, password, swift_code_up)


        # The data returned by select_reversals is assumed to be in a suitable format for JSON serialization

        return Response(data, status=status.HTTP_200_OK)
class ExceptionsView(APIView):
    """
    Retrieve exceptions data.
    """

    def get(self, request, swift_code_up, *args, **kwargs):
        # Use values from .env for database connection
      

        data = select_exceptions(server, database, username, password, swift_code_up)

        # The data returned by select_exceptions is assumed to be in a suitable format for JSON serialization

        return Response(data, status=status.HTTP_200_OK)
    
class ReconciledDataView(APIView):
    """
    Retrieve reconciled data.
    """

    def get(self, request, *args, **kwargs):
        global reconciled_data

        if reconciled_data is not None:
            reconciled_data_cleaned = unserializable_floats(reconciled_data)
            data = reconciled_data_cleaned.to_dict(orient='records')
            return Response(data, status=status.HTTP_200_OK)
        else:
            raise Http404("Reconciled data not found")
        
class UnReconciledDataView(APIView):
    """
    Retrieve unreconciled data.
    """

    def get(self, request, *args, **kwargs):
        global succunreconciled_data

        if succunreconciled_data is not None:
            
            # reconciled_data_cleaned = unserializable_floats(reconciled_data)
            # data = reconciled_data_cleaned.to_dict(orient='records')

            unreconciled_data_cleaned = unserializable_floats(succunreconciled_data)
            data =  unreconciled_data_cleaned.to_dict(orient='records')
            return Response(data, status=status.HTTP_200_OK)
        else:
            raise Http404("unReconciled data not found")  

class ReconStatsView(APIView):

    
    def get(self, request, Swift_code_up):
            
            try:
                # Assume recon_stats_req returns a list of dictionaries or None
                data = recon_stats_req(server, database, username, password, Swift_code_up)
                if data is None:
                    return Response({'error': 'No data found'}, status=status.HTTP_404_NOT_FOUND)              
                
                return Response(data, status=status.HTTP_200_OK)

            except Exception as e:
                return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class sabsreconcile_csv_filesView(APIView):

    serializer_class = SabsSerializer

    def post(self, request):
        serializer = self.serializer_class(data=request.data)
        if serializer.is_valid():
            uploaded_file = serializer.validated_data['file']
            batch_number = serializer.validated_data['batch_number']

            # Save the uploaded file temporarily
            temp_file_path = "temp_file.xlsx"
            with open(temp_file_path, "wb") as buffer:
                buffer.write(uploaded_file.read())

            try:
                # Assume setleSabs returns dataframes as one of its outputs
                _, matched_setle, _, unmatched_setlesabs = setleSabs(temp_file_path, batch_number)
                
                # Perform clean up: remove the temporary file after processing
                os.remove(temp_file_path)

                matched_csv = matched_setle.to_csv(index=False)
                unmatched_csv = unmatched_setlesabs.to_csv(index=False)

                # Create a zip file in memory
                memory_file = BytesIO()
                with ZipFile(memory_file, 'w') as zf:
                    zf.writestr('matched_setle.csv', matched_csv)
                    zf.writestr('unmatched_setlesabs.csv', unmatched_csv)
                
                # u will figure ou how to retun a zipped file here
                memory_file.seek(0)

                response = FileResponse(memory_file, content_type='application/zip')
                response['Content-Disposition'] = 'attachment; filename=Settlement_.zip'
                return response
            
            # this one below doesnt return a zipped file. gets a decode error
                # memory_file.seek(0)

                # response = Response(memory_file, content_type='application/zip')
                # response['Content-Disposition'] = 'attachment; filename=Settlement_recon_files.zip'
                # return response

            except Exception as e:
                # If there's an error during the process, ensure the temp file is removed
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)
                
                # Return error as response
                return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class SettlementView(APIView):
    serializer_class = SettlementSerializer

    def post(self, request):
        serializer = self.serializer_class(data=request.data)
        if serializer.is_valid():
            batch_number = serializer.validated_data['batch_number']

            try:
                # Assume the settle function is defined and available here
                settlement_result = settle(batch_number)

                # Handle case where no records were found or an error occurred in settle
                if settlement_result is None or settlement_result.empty:
                    return Response({"detail": "No records for processing found or an error occurred."},
                                    status=status.HTTP_400_BAD_REQUEST)

                # Convert the DataFrame to CSV
                settlement_csv = settlement_result.to_csv(index=False)

                # Create a zip file in memory
                memory_file = BytesIO()
                with ZipFile(memory_file, 'w') as zf:
                    zf.writestr('settlement_result.csv', settlement_csv)

                memory_file.seek(0)

                response = FileResponse(memory_file, content_type='application/zip')
                response['Content-Disposition'] = 'attachment; filename=Settlement_.zip'
                return response

            except Exception as e:
                # Handle other unexpected errors
                return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)