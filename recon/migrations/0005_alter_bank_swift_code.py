# Generated by Django 4.2.5 on 2023-10-01 20:36

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('recon', '0004_bank_bank_code'),
    ]

    operations = [
        migrations.AlterField(
            model_name='bank',
            name='swift_code',
            field=models.CharField(max_length=10, unique=True),
        ),
    ]
