from django.contrib import admin
from .models import Bank,UserBankMapping

# Register your models here.
class BankAdmin(admin.ModelAdmin):
    list_display = ["name","swift_code"]

class MappedUserAdmin(admin.ModelAdmin):
    list_display = ["bank","user"]
admin.site.register(UserBankMapping,MappedUserAdmin)


