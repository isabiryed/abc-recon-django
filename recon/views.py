from django.shortcuts import render
from rest_framework import generics
from .models import Reconciliation,ReconciliationLog
from .serializers import ReconciliationSerializer
# Create your views here.

class ReconciliationListView(generics.ListCreateAPIView):
    queryset = Reconciliation.objects.all()
    serializer_class = ReconciliationSerializer

class ReconciliationLogListView(generics.ListAPIView):
    queryset = ReconciliationLog.objects.all()
