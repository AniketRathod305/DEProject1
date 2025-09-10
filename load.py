from transform import transform_data
from connection import mysql_connect


def load_data():

    df = transform_data()
    conn = mysql_connect()
    cursor = conn.cursor()

    ddl_query = '''create table if not exists employee_data(
                   employee_id VARCHAR(30) PRIMARY KEY,
                   fullname VARCHAR(50),
                   department VARCHAR(30),
                   job_title VARCHAR(30),
                   hire_date DATE,
                   city VARCHAR(50),
                   country VARCHAR(100),
                   status VARCHAR(20),
                   workmode VARCHAR(20),
                   performance_rating INT,
                   experience_years INT,
                   salary DOUBLE)'''
    try:
        cursor.execute(ddl_query)
        print('Table created successfully')
    except Exception as e:
        print('Error occured', e)

    insert_query = '''insert into employee_data(employee_id,fullname,department,job_title,hire_date,city,country,status,workmode,performance_rating,experience_years,salary) 
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
    as new 
    on duplicate key update 
    fullname=new.fullname,
    department = new.department,
    job_title = new.job_title,
    hire_date = new.hire_date,
    city = new.city,
    country = new.country,
    status = new.status,
    workmode = new.workmode,
    performance_rating=new.performance_rating,
    experience_years = new.experience_years,
    salary=new.salary'''

    columns = [
        "Employee_ID", "Full_Name", "Department", "Job_Title", "Hire_Date", "City", "Country",
        "Status", "Work_Mode", "Performance_Rating", "Experience_Years", "Salary_INR"
    ]

    # Reorder dataframe and convert to list of tuples
    data_tuples = list(df[columns].itertuples(index=False, name=None))

    # Bulk insert using executemany
    batch_size = 1000
    try:
        for i in range(0, len(data_tuples), batch_size):
            cursor.executemany(insert_query, data_tuples[i:i+batch_size])
            print('Rows insert/update completed')
            flag = 1
    except Exception as e:
        print('Error occured', e)
        flag = 0

    # Commit and close
    conn.commit()
    cursor.close()
    conn.close()
    return flag
