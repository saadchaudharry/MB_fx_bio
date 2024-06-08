import json
import pyodbc
import datetime
from frappeclient import FrappeClient
import schedule
import time

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

today = datetime.datetime.now().date().strftime('%Y-%m-%d')

# def serialize_datetime(obj):
#     if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
#         return obj.isoformat()
#     raise TypeError("Type not serializable")


def serialize_datetime(obj):
    if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
        return obj.isoformat()
    raise TypeError("Type not serializable")




def job():

  if db['today'] == 0:
        today = db['file_name']
  today = datetime.datetime.now().date().strftime('%Y-%m-%d')

  server = db['server']
  database = db['database']
  username = db['username']
  password = db['password']

  query = f"""
              SELECT 
                     emp_code as unique_id,
                     CAST(punch_time as DATE) as date,
                     MIN(CAST(punch_time as TIME)) as in_time,
                     MAX(CAST(punch_time as TIME)) as out_time,
                     MIN(punch_time) as first_punch_time, -- Aggregate function on punch_time
                       MAX(punch_time) as last_punch_time, -- Aggregate function on punch_time
                       MIN(punch_state) as first_punch_state, -- Assuming you want the state for the first punch time
                       MAX(punch_state) as last_punch_state -- Assuming you want the state for the last punch time
                  FROM [wdms].[dbo].[iclock_transaction] iclock
                  WHERE emp_code LIKE '24%' and CAST(punch_time as DATE) = CAST(GETDATE() as DATE)
                  GROUP BY
                       iclock.emp_code,
                       CAST(iclock.punch_time as DATE)
                """

    # Establishing a connection to the SQL Server
  cnxn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};\
                        SERVER='+server+';\
                        DATABASE='+database+';\
                        UID='+username+';\
                        PWD=' + password)

  cursor = cnxn.cursor()
  data = []

  result = cursor.execute(query)
  columns = [column[0] for column in cursor.description]

  for row in result:
      data.append(dict(zip(columns, row)))

  print(data)
  dummy_data = json.dumps({"data": data}, default=serialize_datetime)

  with open(f"data_{today}.json", 'w+') as datafile:
      json.dump(json.loads(dummy_data), datafile, indent=4)

  print('call')
  conn = FrappeClient(db['url'])
  conn.login(db['email'], db["app_password"])

  results = []
  today = datetime.datetime.now().date().strftime('%Y-%m-%d')

  for i in data:
      i['doctype'] = 'Biometric Attendance'
      i['first_punch_time'] = '' 
      i['last_punch_time'] = '' 
      i['last_punch_time'] = '' 
      i["name"] = f"""BA-{i["unique_id"]}-{str(i["date"])}""",
      i['youth_id'] = str(i['unique_id'])

      i['date'] = str(i['date'])
      i['in_time'] = str(i['in_time'])
      i['out_time'] = str(i['out_time'])
      print(i)
      try:
          response = conn.insert(i)
          results.append({'data': i, 'response': response})
      except Exception as e:
          results.append({'data': i, 'error': str(e)})

  with open(f"results_{today}.json", 'w+') as result_file:
      json.dump(results, result_file, indent=4, default=serialize_datetime)

job()

# Schedule the job to run every day at 10:30 PM
schedule.every().day.at(db['time']).do(job)

while True:
    print('running')
    schedule.run_pending()
    time.sleep(1)
