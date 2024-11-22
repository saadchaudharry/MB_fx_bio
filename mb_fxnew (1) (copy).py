import json
import pyodbc
import datetime
import requests
import schedule
import time

# Database and API configuration
db = {
    "server": "LIVELIHOODPROJ",
    "database": "wdms",
    "username": "abacus",
    "password": "admin@123",
    "url": "https://magicbus.frappe.cloud",
    "email": "rohit@magicbusindia.org",
    "app_password": "rohit@123",
    "today": 1,
    "file_name": "file_name",
    "time": "22:02",
    "date_range": {
        "start_date": "",
        "end_date": ""
    }
}

# Serialize datetime objects for JSON
def serialize_datetime(obj):
    if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
        return obj.isoformat()
    raise TypeError("Type not serializable")

# Frappe login using requests and get the authorization cookie
def frappe_login():
    login_url = f"{db['url']}/api/method/login"
    login_data = {
        'usr': db['email'],
        'pwd': db['app_password']
    }
    
    try:
        response = requests.post(login_url, data=login_data)
        response.raise_for_status()
        cookies = response.cookies
        print("Frappe login successful.")
        return cookies
    except requests.exceptions.RequestException as e:
        print(f"Frappe login failed: {e}")
        return None

# Fetch data from SQL Server
def fetch_sql_data():
    server = db['server']
    database = db['database']
    username = db['username']
    password = db['password']

    # SQL query to fetch biometric data
    query =f"""
              SELECT 
                     emp_code as unique_id,
                     CAST(punch_time as DATE) as date,
                     MIN(CAST(punch_time as TIME)) as in_time,
                     MAX(CAST(punch_time as TIME)) as out_time,
                     MIN(punch_time) as first_punch_time, -- Aggregate function on punch_time
                       MAX(punch_time) as last_punch_time, -- Aggregate function on punch_time
                       MIN(punch_state) as first_punch_state, -- Assuming you want the state for the first punch time
                       MAX(punch_state) as last_punch_state  
                  FROM [wdms].[dbo].[iclock_transaction] iclock
                  WHERE emp_code LIKE '24%' and CAST(punch_time as DATE) = CAST(GETDATE() as DATE)
                  GROUP BY
                       iclock.emp_code,
                       CAST(iclock.punch_time as DATE)
    """

    # Establish connection to SQL Server
    cnxn = pyodbc.connect(f'DRIVER={{SQL Server Native Client 11.0}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
    cursor = cnxn.cursor()
    
    data = []
    result = cursor.execute(query)
    columns = [column[0] for column in cursor.description]

    # Fetch the data and return as a list of dictionaries
    for row in result:
        data.append(dict(zip(columns, row)))
    
    return data

# Insert data into Frappe using requests module
def insert_data_to_frappe(cookies, data):
    results = []
    insert_url = f"{db['url']}/api/resource/Biometric%20Attendance"

    for i in data:
        i['first_punch_time'] = str(i['first_punch_time'])
        i['last_punch_time'] = str(i['last_punch_time'])
        i['youth_id'] = str(i['unique_id'])
        i['date'] = str(i['date'])
        i['in_time'] = str(i['in_time'])
        i['out_time'] = str(i['out_time'])

        # Send data to Frappe
        try:
            response = requests.post(insert_url, json=i, cookies=cookies)
            response.raise_for_status()
            results.append({'data': i, 'response_code': response.status_code, 'response': response.json()})
        except requests.exceptions.RequestException as e:
            results.append({'data': i, 'response_code': response.status_code if response else 500, 'error': str(e)})
    
    return results

# Job function that gets executed on schedule
def job():
    today = db['file_name'] if db['today'] == 0 else datetime.datetime.now().strftime('%Y-%m-%d')

    # Fetch data from SQL Server
    data = fetch_sql_data()

    # Save the fetched data to a JSON file
    with open(f"data_{today}.json", 'w+') as datafile:
        json.dump(data, datafile, indent=4, default=serialize_datetime)

    # Log into Frappe
    cookies = frappe_login()

    if cookies:
        # Insert data into Frappe
        results = insert_data_to_frappe(cookies, data)

        # Save the insertion results with response codes
        with open(f"results_{today}.json", 'w+') as result_file:
            json.dump(results, result_file, indent=4, default=serialize_datetime)
    else:
        print("Skipping data insertion due to Frappe login failure.")

# Schedule the job to run every day at the specified time
schedule.every().day.at(db['time']).do(job)


job()


# Keep the script running and check for scheduled jobs
# Schedule the job to run every day at the specified time

while True:
    print('running')
    schedule.run_pending()
    time.sleep(1)
