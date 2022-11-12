
import json
import logging

import pandas as pd
from backend.utils.redshif import Redshift
from rest_framework import status
from rest_framework.renderers import TemplateHTMLRenderer
from rest_framework.response import Response
from rest_framework.views import APIView
from django.urls import reverse
from django.shortcuts import render
from django.contrib import messages
from django.http import HttpResponseRedirect
import redshift_connector
# conn = Redshift().conn
_logger = logging.getLogger(__name__)
# Create your views here.

class EmployeeView(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'employees.html'
    conn = Redshift().conn
    conn.autocommit = True
    conn.run("VACUUM")
    def get(self, request):
        _logger.info('Start Get employees')
        # Create a Cursor object
        _logger.info('Connect to Redshif')
        cursor = self.conn.cursor()

        # Query a table using the Cursor
        cursor.execute("SELECT p.business_entity_id,p.first_name,p.last_name,e.birth_date,e.salaried_flag,e.vacation_hours,e.sick_leave_hours \
                        FROM adventureworks2008r2_person.person p \
                        INNER JOIN adventureworks2008r2_humanresources.employee e ON p.business_entity_id=e.business_entity_id;")
        self.conn.autocommit = False               
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
    conn = Redshift().conn
    conn.autocommit = True
    conn.run("VACUUM")
    def get(self, request, id):
        _logger.info('Start Get detail employee')
        # Create a Cursor object
        _logger.info('Connect to Redshif')
    
        cursor = self.conn.cursor()

        # Query a table using the Cursor
        cursor.execute(f"SELECT p.business_entity_id,p.first_name,p.last_name,e.birth_date,e.salaried_flag,e.vacation_hours,e.sick_leave_hours,e.hire_date,e.gender,e.job_title,dt.name,d.start_date,d.end_date \
                        FROM adventureworks2008r2_person.person p \
                        INNER JOIN adventureworks2008r2_humanresources.employee e ON p.business_entity_id=e.business_entity_id \
                        INNER JOIN adventureworks2008r2_humanresources.employee_department_history d ON p.business_entity_id=d.business_entity_id \
                        INNER JOIN adventureworks2008r2_humanresources.department dt ON d.department_id=dt.department_id    \
                        WHERE p.business_entity_id={id};")
        self.conn.autocommit = False               
        #Retrieve the query result set
        
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


class DepartmentView(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'departments.html'
    conn = Redshift().conn
    conn.autocommit = True
    conn.run("VACUUM")
    def get(self, request):
        _logger.info('Start Get deparment')
        self.http_method_names.append("GET")
        # Create a Cursor object
        _logger.info('Connect to Redshif')
        cursor = self.conn.cursor()

        # Query a table using the Cursor
        cursor.execute("SELECT p.department_id,p.name,p.group_name,COUNT(*) employess \
                        FROM adventureworks2008r2_humanresources.department p \
                        RIGHT JOIN adventureworks2008r2_humanresources.employee_department_history e ON p.department_id=e.department_id \
                        INNER JOIN adventureworks2008r2_humanresources.employee em ON e.business_entity_id=em.business_entity_id \
                        GROUP BY p.department_id,p.name,p.group_name;")
        self.autocommit = False           
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
