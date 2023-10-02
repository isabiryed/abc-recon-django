from django.urls import path

from recon.views import ExceptionsView, ReconStatsView, ReconcileView, ReconciledDataView, ReversalsView, SettlementView, UnReconciledDataView, sabsreconcile_csv_filesView
urlpatterns = [
    path('reconcile/', ReconcileView.as_view(), name='reconcile'),
    path('reconstats/<str:Swift_code_up>/', ReconStatsView.as_view(), name='reconstats'),
    path('reversals/<str:swift_code_up>/', ReversalsView.as_view(), name='reversals'),  # Add this line
    path('exceptions/<str:swift_code_up>/', ExceptionsView.as_view(), name='exceptions'),
    path('reconcileddata/', ReconciledDataView.as_view(), name='reconcileddata'),
    path('unreconcileddata/', UnReconciledDataView.as_view(), name='unreconcileddata'),
    path('settlementcsv_files/', SettlementView.as_view(), name='settlement-csv-files'),
    path('sabsreconcile_csv_file/', sabsreconcile_csv_filesView.as_view(), name='ssabsreconcile_csv_file'),



]