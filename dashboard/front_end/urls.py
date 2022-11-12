from django.urls import path, include
from .views import index, home

urlpatterns = [

    path('dashboard', index, name='dashboard'),
]