#Connect to the cluster
import redshift_connector
conn = redshift_connector.connect(
     host='examplecluster.abc123xyz789.us-west-1.redshift.amazonaws.com',
     database='dev',
     user='awsuser',
     password='my_password'
  )
  
# Create a Cursor object
cursor = conn.cursor()

# Query a table using the Cursor
cursor.execute("select * from employee")
                
#Retrieve the query result set
result = cursor.fetchall()
print(result)
 
                
