from rest_framework import serializers
from .models import Bank,Reconciliation,ReconciliationLog

class ReconciliationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Reconciliation
        
class BankSerializer(serializers.ModelSerializer):
    class Meta:
        model = Bank

class LogSerializer(serializers.ModelSerializer):
    class Meta:
        model = ReconciliationLog