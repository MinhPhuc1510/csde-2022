
import json
import logging
import datetime
import uuid

import pandas as pd
import redshift_connector
from backend.utils.redshif import Redshift
from django.contrib import messages
from django.http import HttpResponseRedirect
from django.shortcuts import render
from django.urls import reverse
from backend.forms.employees import EmployeeForm, DepartmentForm
from rest_framework import status
from rest_framework.renderers import TemplateHTMLRenderer
from rest_framework.response import Response
from rest_framework.views import APIView

_logger = logging.getLogger(__name__)
# Create your views here.

class EmployeeView(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'employees.html'
    
    def get(self, request):
        _logger.info('Start Get employees')
        # Create a Cursor object
        _logger.info('Connect to Redshif')
        conn = Redshift().conn
        cursor = conn.cursor()

        # Query a table using the Cursor
        cursor.execute("SELECT p.business_entity_id,p.first_name,p.last_name,e.birth_date,e.salaried_flag,e.vacation_hours,e.sick_leave_hours \
                        FROM adventureworks2008r2_person.person p \
                        INNER JOIN adventureworks2008r2_humanresources.employee e ON p.business_entity_id=e.business_entity_id;")              
        #Retrieve the query result set
        
        query = cursor.fetchall()

        if len(query) == 0:
            respones = {
            'data': None
        }
            return Response(data=respones, status=status.HTTP_404_NOT_FOUND)

        df = pd.DataFrame(query)
        df.columns = ['business_entity_id', 'first_name', 'last_name', 'birth_date', 'salaried_flag', 'vacation_hours', 'sick_leave_hours']
        df['birth_date'] = pd.to_datetime(df['birth_date']).astype(str)
        result = df.to_json(orient="records")
        respones = {
            'data': json.loads(result)
        }
        _logger.info('End Get employees')

        return Response(data=respones, status=status.HTTP_200_OK)


class DetailEmployeeView(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'get_detail_employee.html'
    
    # conn.autocommit = True
    # conn.run("VACUUM")
    def get(self, request, id):
        _logger.info('Start Get detail employee')
        # Create a Cursor object
        _logger.info('Connect to Redshif')
        conn = Redshift().conn
        cursor = conn.cursor()

        # Query a table using the Cursor
        cursor.execute(f"SELECT p.business_entity_id,p.first_name,p.last_name,e.birth_date,e.salaried_flag,e.vacation_hours,e.sick_leave_hours,e.hire_date,e.gender,e.job_title,dt.name,d.start_date,d.end_date, py.rate \
                        FROM adventureworks2008r2_person.person p \
                        INNER JOIN adventureworks2008r2_humanresources.employee e ON p.business_entity_id=e.business_entity_id \
                        INNER JOIN adventureworks2008r2_humanresources.employee_department_history d ON p.business_entity_id=d.business_entity_id \
                        INNER JOIN adventureworks2008r2_humanresources.department dt ON d.department_id=dt.department_id    \
                        INNER JOIN adventureworks2008r2_humanresources.employee_pay_history py ON p.business_entity_id=py.business_entity_id \
                        WHERE p.business_entity_id={id};")
        
        query = cursor.fetchall()
        if len(query) == 0:
            respones = {
            'data': None
        }
            return Response(data=respones, status=status.HTTP_404_NOT_FOUND)
        df = pd.DataFrame(query)
        df.columns = ['business_entity_id', 'first_name', 'last_name', 'birth_date', 'salaried_flag', 'vacation_hours', 'sick_leave_hours', 'hire_date', 'gender', 'job_title', 'name', 'start_date', 'end_date', 'rate']
        df['birth_date'] = pd.to_datetime(df['birth_date']).astype(str)
        df['hire_date'] = pd.to_datetime(df['hire_date']).astype(str)
        df['start_date'] = pd.to_datetime(df['start_date']).astype(str)
        df['end_date'] = pd.to_datetime(df['end_date']).astype(str)

        result = df.to_json(orient="records")
        respones = {
            'data': json.loads(result)
        }
        _logger.info('End Get detail employee')

        return Response(data=respones, status=status.HTTP_200_OK)

def alert_delete(request, id):
  contex = {'id': id}
  return render(request, 'alert_delete.html', contex)

def delete_employee(request, id):
    _logger.info('Start Delete detail employee')
    # Create a Cursor object
    _logger.info('Connect to Redshif')
    conn = Redshift().conn
    conn.autocommit = True
    conn.run("VACUUM")
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"DELETE FROM adventureworks2008r2_humanresources.employee \
        WHERE business_entity_id={id};")
        conn.autocommit = False          
    except redshift_connector.error.ProgrammingError :
        messages.info(request, 'Failed!')
        return HttpResponseRedirect(reverse('employees'))
    messages.info(request, 'Delete successfully!')
    return HttpResponseRedirect(reverse('employees'))

def update_employee(request, id):
    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        # create a form instance and populate it with data from the request:
        form = EmployeeForm(request.POST)
        # check whether it's valid:
        if form.is_valid():
            conn = Redshift().conn
            conn.autocommit = True
            conn.run("VACUUM")
            cursor = conn.cursor()
            value = ','.join(f"{key}='{form.cleaned_data[key]}'" for key in form.cleaned_data if key in ['birth_date', 'hire_date', 'gender', 'job_title'])
            query = f"UPDATE adventureworks2008r2_humanresources.employee \
                    SET {value} \
                    WHERE business_entity_id={id};"
            cursor.execute(query)
            

            value_2 = ','.join(f"{key}='{form.cleaned_data[key]}'" for key in form.cleaned_data if key in ['first_name', 'last_name'])
            query_2 = f"UPDATE adventureworks2008r2_person.person \
                    SET {value_2} \
                    WHERE business_entity_id={id};"
            cursor.execute(query_2)

            value_3 = ','.join(f"{key}='{form.cleaned_data[key]}'" for key in form.cleaned_data if key in ['salary'])
            value_3 = value_3.replace('salary', 'rate')
            query_3 = f"UPDATE adventureworks2008r2_humanresources.employee_pay_history \
                    SET {value_3} \
                    WHERE business_entity_id={id};"
            cursor.execute(query_3)
            conn.autocommit = False

            messages.info(request, 'Update sucessfully')
            return HttpResponseRedirect(reverse('detai_employee',args=(id,)))
    
    # if a GET (or any other method) we'll create a blank form
    elif request.method == 'GET':
        conn = Redshift().conn
        cursor = conn.cursor()

        # Query a table using the Cursor
        cursor.execute(f"SELECT p.business_entity_id,p.first_name,p.last_name,e.birth_date,e.salaried_flag,e.vacation_hours,e.sick_leave_hours,e.hire_date,e.gender,e.job_title,dt.name,d.start_date,d.end_date \
                        FROM adventureworks2008r2_person.person p \
                        INNER JOIN adventureworks2008r2_humanresources.employee e ON p.business_entity_id=e.business_entity_id \
                        INNER JOIN adventureworks2008r2_humanresources.employee_department_history d ON p.business_entity_id=d.business_entity_id \
                        INNER JOIN adventureworks2008r2_humanresources.department dt ON d.department_id=dt.department_id    \
                        WHERE p.business_entity_id={id};")
        
        query = cursor.fetchall()
        if len(query) == 0:
            respones = {
            'data': None
        }
            return Response(data=respones, status=status.HTTP_404_NOT_FOUND)
        df = pd.DataFrame(query)
        df.columns = ['business_entity_id', 'first_name', 'last_name', 'birth_date', 'salaried_flag', 'vacation_hours', 'sick_leave_hours', 'hire_date', 'gender', 'job_title', 'name', 'start_date', 'end_date']
        df['birth_date'] = pd.to_datetime(df['birth_date']).astype(str)
        df['hire_date'] = pd.to_datetime(df['hire_date']).astype(str)
        df['start_date'] = pd.to_datetime(df['start_date']).astype(str)
        df['end_date'] = pd.to_datetime(df['end_date']).astype(str)

        result = json.loads(df.to_json(orient="records"))[0]    
        form_data = {
            "first_name": result['first_name'],
            "last_name": result['last_name'],
            "birth_date": result['birth_date'],
            "gender": result['gender'],
            "hire_date": result['hire_date'],
            "job_title:": result['job_title']
        }
        
        form_data = EmployeeForm(form_data)

    return render(request, 'employee_form.html', {'form': form_data, 'id':id})

class DepartmentView(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'departments.html'
   
    def get(self, request):
        _logger.info('Start Get deparment')
        self.http_method_names.append("GET")
        # Create a Cursor object
        _logger.info('Connect to Redshif')
        conn = Redshift().conn
        cursor = conn.cursor()

        # Query a table using the Cursor
        cursor.execute("SELECT p.department_id,p.name,p.group_name,COUNT(*) employess \
                        FROM adventureworks2008r2_humanresources.department p \
                        RIGHT JOIN adventureworks2008r2_humanresources.employee_department_history e ON p.department_id=e.department_id \
                        INNER JOIN adventureworks2008r2_humanresources.employee em ON e.business_entity_id=em.business_entity_id \
                        GROUP BY p.department_id,p.name,p.group_name;")
         
        #Retrieve the query result set
        
        query = cursor.fetchall()
        if len(query) == 0:
            respones = {
            'data': None
        }
            return Response(data=respones, status=status.HTTP_404_NOT_FOUND)
        df = pd.DataFrame(query)
        df.columns = ['department_id', 'name', 'group_name', 'employess']
        result = df.to_json(orient="records")
        respones = {
            'data': json.loads(result)
        }
        _logger.info('End Get departments')

        return Response(data=respones, status=status.HTTP_200_OK)


def delete_employee(request, id):
    _logger.info('Start Delete detail employee')
    # Create a Cursor object
    _logger.info('Connect to Redshif')
 
    try:
        conn = Redshift().conn
        conn.autocommit = True
        conn.run("VACUUM")
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM adventureworks2008r2_humanresources.employee \
        WHERE business_entity_id={id};")

        cursor.execute(f"DELETE FROM adventureworks2008r2_humanresources.employee_department_history \
        WHERE business_entity_id={id};")
        conn.autocommit = False

    except redshift_connector.error.ProgrammingError :
        messages.info(request, 'Failed!')
        return HttpResponseRedirect(reverse('employees'))
    messages.info(request, 'Delete successfully!')
    return HttpResponseRedirect(reverse('employees'))

def update_employee_department(request, id):
    # if this is a POST request we need to process the form data
    conn = Redshift().conn
    conn.autocommit = True
    conn.run("VACUUM")
    cursor = conn.cursor()
    if request.method == 'POST':
        # create a form instance and populate it with data from the request:
        form = DepartmentForm(request.POST)
        
        # check whether it's valid:
        if form.is_valid():
            
            query = f"SELECT department_id \
                    FROM adventureworks2008r2_humanresources.department \
                    WHERE name='{form.cleaned_data['department']}';"

            cursor.execute(query)

            result = cursor.fetchall()
            value = ','.join(f"{key}='{form.cleaned_data[key]}'" for key in form.cleaned_data if key!='department')
            value = value + f',department_id={result[0][0]}'
            query = f"UPDATE adventureworks2008r2_humanresources.employee_department_history \
                    SET {value} \
                    WHERE business_entity_id={id};"
            cursor.execute(query)
            conn.autocommit = False

            messages.info(request, 'Update sucessfully')
            return HttpResponseRedirect(reverse('detai_employee',args=(id,)))
    
    # if a GET (or any other method) we'll create a blank form
    elif request.method == 'GET':
        # Query a table using the Cursor
        cursor.execute(f"SELECT p.business_entity_id,p.first_name,p.last_name,e.birth_date,e.salaried_flag,e.vacation_hours,e.sick_leave_hours,e.hire_date,e.gender,e.job_title,dt.name,d.start_date,d.end_date \
                        FROM adventureworks2008r2_person.person p \
                        INNER JOIN adventureworks2008r2_humanresources.employee e ON p.business_entity_id=e.business_entity_id \
                        INNER JOIN adventureworks2008r2_humanresources.employee_department_history d ON p.business_entity_id=d.business_entity_id \
                        INNER JOIN adventureworks2008r2_humanresources.department dt ON d.department_id=dt.department_id    \
                        WHERE p.business_entity_id={id};")
        
        query = cursor.fetchall()
        if len(query) == 0:
            return HttpResponseRedirect(reverse('detai_employee',args=(id,)))
        df = pd.DataFrame(query)
        df.columns = ['business_entity_id', 'first_name', 'last_name', 'birth_date', 'salaried_flag', 'vacation_hours', 'sick_leave_hours', 'hire_date', 'gender', 'job_title', 'name', 'start_date', 'end_date']
        df['birth_date'] = pd.to_datetime(df['birth_date']).astype(str)
        df['hire_date'] = pd.to_datetime(df['hire_date']).astype(str)
        df['start_date'] = pd.to_datetime(df['start_date']).astype(str)
        df['end_date'] = pd.to_datetime(df['end_date']).astype(str)

        result = json.loads(df.to_json(orient="records"))[0]    
        form_data = {
            "name": result['name'],
            "start_date": result['start_date'],
            "end_date": result['end_date'],
        }
        
        form_data = DepartmentForm(form_data)

        return render(request, 'department_employee_form.html', {'form': form_data, 'id':id})