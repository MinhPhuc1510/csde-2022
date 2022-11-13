from django import forms
from backend.utils.redshif import Redshift

CHOICE_GENDER = [
    ('M', 'MALE'),
    ('F', 'FEMALE')
]

def get_department():
        conn = Redshift().conn
        cursor = conn.cursor()
        cursor.execute("SELECT  p.name \
                                FROM adventureworks2008r2_humanresources.department p;")
        result = [(i[0],i[0]) for i in cursor.fetchall()]
        return result

class EmployeeForm(forms.Form):
    first_name = forms.CharField(max_length=200)
    last_name = forms.CharField(max_length=200)
    birth_date = forms.DateField(widget=forms.SelectDateWidget(years=range(1900,3000),empty_label=None))
    gender = forms.CharField(widget=forms.Select(choices=CHOICE_GENDER))
    hire_date =  forms.DateField(widget=forms.SelectDateWidget(years=range(1900,3000),empty_label=None))
    job_title = forms.CharField(max_length=200)
    salary = forms.FloatField()

class DepartmentForm(forms.Form):
    department = forms.CharField(widget=forms.Select(choices=get_department()))
    start_date = forms.DateField(widget=forms.SelectDateWidget(years=range(1900,3000),empty_label=None))
    # end_date = forms.DateField(widget=forms.SelectDateWidget(years=range(1900,3000), empty_label=None))

    