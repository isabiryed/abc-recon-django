# Generated by Django 4.2.5 on 2023-10-01 20:07

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('recon', '0003_reconciliationlog_delete_reconciliationlogs'),
    ]

    operations = [
        migrations.AddField(
            model_name='bank',
            name='bank_code',
            field=models.CharField(max_length=10, null=True, unique=True),
        ),
    ]
