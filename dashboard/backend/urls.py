from django.urls import path
from .views import EmployeeView, DetailEmployeeView, DepartmentView, delete_employee, alert_delete

urlpatterns = [

    path('humanresources/employees', EmployeeView.as_view(), name='employees'),
    path('humanresources/employees/<int:id>', DetailEmployeeView.as_view(), name='detai_employee'),
    path('humanresources/employees/<int:id>/delete', delete_employee, name='delete_employee'),
     path('humanresources/employees/<int:id>/alert', alert_delete, name='alert_delete_employee'),
    path('humanresources/departments', DepartmentView.as_view(), name='departments'),
]