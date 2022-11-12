from email.policy import HTTP
from django.shortcuts import render

# Create your views here.

def index(request):
  # iframe = "https://app.powerbi.com/reportEmbed?reportId=12ac76ce-365c-4792-88ad-51c30b10eb49&autoAuth=true&ctid=7bbbced8-b31a-4a36-95bb-9f06bc9d72a6"
  iframe = "https://app.powerbi.com/reportEmbed?reportId=7dee1973-f2d5-4093-8b85-d7931e3ca7e2&autoAuth=true&ctid=7bbbced8-b31a-4a36-95bb-9f06bc9d72a6"
  context = {'iframe': iframe}
  return render(request, 'Dashboard.html', context)

def home(request):
  iframe = "https://app.powerbi.com/reportEmbed?reportId=7dee1973-f2d5-4093-8b85-d7931e3ca7e2&autoAuth=true&ctid=7bbbced8-b31a-4a36-95bb-9f06bc9d72a6"
  context = {'iframe': iframe}
  return render(request, 'home.html', context)