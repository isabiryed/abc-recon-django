# Generated by Django 4.2.5 on 2023-10-01 18:43

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('recon', '0002_reconciliation_reconciliationlogs'),
    ]

    operations = [
        migrations.CreateModel(
            name='ReconciliationLog',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('date_time', models.DateTimeField(blank=True, db_column='DATE_TIME', null=True)),
                ('recon_id', models.CharField(blank=True, db_column='RECON_ID', max_length=35, null=True)),
                ('bank_id', models.CharField(blank=True, db_column='BANK_ID', max_length=15, null=True)),
                ('user_id', models.CharField(blank=True, db_column='USER_ID', max_length=35, null=True)),
                ('rq_date_range', models.CharField(blank=True, db_column='RQ_DATE_RANGE', max_length=255, null=True)),
                ('upld_rws', models.CharField(blank=True, db_column='UPLD_RWS', max_length=15, null=True)),
                ('rq_rws', models.CharField(blank=True, db_column='RQ_RWS', max_length=15, null=True)),
                ('recon_rws', models.CharField(blank=True, db_column='RECON_RWS', max_length=15, null=True)),
                ('unrecon_rws', models.CharField(blank=True, db_column='UNRECON_RWS', max_length=15, null=True)),
                ('excep_rws', models.CharField(blank=True, db_column='EXCEP_RWS', max_length=15, null=True)),
                ('feedback', models.TextField(blank=True, db_column='FEEDBACK', null=True)),
            ],
            options={
                'db_table': 'Reconciliationlogs',
                'managed': False,
            },
        ),
        migrations.DeleteModel(
            name='Reconciliationlogs',
        ),
    ]
