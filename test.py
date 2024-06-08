import json
import pyodbc
import datetime
from frappeclient import FrappeClient
import schedule
import time

with open('db.json') as f:
    db = json.load(f)

today = datetime.datetime.now().date().strftime('%Y-%m-%d')

if db['today'] == 0:
    today = db['file_name']

server   = db['server']
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

def serialize_datetime(obj):
    if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
        return obj.isoformat()
    raise TypeError("Type not serializable")

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

def job(data):
    print('call')
    conn = FrappeClient(db['url'])
    conn.authenticate(db['api_key'], db["api_secret"])
    results = []

    for i in data:
        i['doctype'] = 'Biometric Attendance'
        try:
            response = conn.insert(i)
            results.append({'data': i, 'response': response})
        except Exception as e:
            results.append({'data': i, 'error': str(e)})

    with open(f"results_{today}.json", 'w') as result_file:
        json.dump(results, result_file, indent=4)

# Schedule the job to run every day at 10:30 PM
schedule.every().day.at(db['time']).do(lambda: job(data))

while True:
	schedule.run_pending()
	time.sleep(1)













































































































































# import json
# import pyodbc
# import datetime
# from frappeclient import FrappeClient
# import schedule
# import time


# with open('db.json') as f:
# 	db = json.load(f)


# today = datetime.datetime.now().date().strftime('%Y-%m-%d')

# if db['today'] == 0:
# 	today = db['file_name']

# server   = db['server']
# database = db['database']
# username = db['username']
# password = db['password']


# query = f"""
# 				SELECT 
# 					   emp_code as unique_id,
# 					   CAST(punch_time as DATE) as date,
# 					   MIN(CAST(punch_time as TIME)) as in_time,
# 					   MAX(CAST(punch_time as TIME)) as out_time,
# 					   MIN(punch_time) as first_punch_time, -- Aggregate function on punch_time
# 					   MAX(punch_time) as last_punch_time, -- Aggregate function on punch_time
# 					   MIN(punch_state) as first_punch_state, -- Assuming you want the state for the first punch time
# 					   MAX(punch_state) as last_punch_state -- Assuming you want the state for the last punch time
# 				  FROM [wdms].[dbo].[iclock_transaction] iclock
# 				  WHERE emp_code LIKE '24%' and CAST(punch_time as DATE) = CAST(GETDATE() as DATE)
# 				  GROUP BY
# 					   iclock.emp_code,
# 					   CAST(iclock.punch_time as DATE)
# 			  """

# def serialize_datetime(obj):
#     if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
#         return obj.isoformat()
#     raise TypeError("Type not serializable")


# # Establishing a connection to the SQL Server
# cnxn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};\
#                       SERVER='+server+';\
#                       DATABASE='+database+';\
#                       UID='+username+';\
#                       PWD=' + password)

# cursor = cnxn.cursor()


# data = []


# result = cursor.execute(f"""{query}""")

# columns = [column[0] for column in cursor.description]

# for row in result:
#     data.append(dict(zip(columns, row)))



# print(data)



# def job(data):
# 	print('call')
# 	conn = FrappeClient(db['url'])
# 	conn.authenticate(db['api_key'], db["api_secret"])
# 	results = []

# 	for i in data:
# 		json_data = json.dumps(i, default=serialize_datetime)
# 		print(json_data)
# 		print(type(json_data))
# 		# try:
# 		json_data['doctype'] = 'Biometric Attendance'
# 		# unique_id = data['youth_master'].split('-')
# 		# # data['unique_id'] = unique_id[1]+unique_id[2]
# 		# data['date'] = today
# 		json_data['youth_master'] = ''

# 		# print(json_data)
# 		# print(type(json_data))

# 		# response = conn.insert(json_data)

# 		# 	results.append({'data': json_data, 'response': response})

# 		# except Exception as e:
# 		# 	results.append({'data': json_data, 'error': str(e)})

# 		# 	print(json_data)
# 		# 	print(type(json_data))

# 		# 	print('\n')


# 	# with open(f"""results_{today}.json""", 'w') as result_file:
# 	# 	json.dump(results, result_file, indent=4)


# job(data)
# print('\n')
# input()






# # dummy_data = json.dumps({"data": data}, default=serialize_datetime)

# # with open(f"""data_{today}.json""", 'w') as datafile:
# # 	json.dump(dummy_data, datafile, indent=4)


# # null = None
# # dummy_data = [
# # 	{
# # 		"youth_id": "2300000489",
# # 		"first_name": "Rajesh",
# # 		"date": "2024-02-20",
# # 		"in_time": "9:00:00",
# # 		"out_time": "23:00:00",
# # 		"upload_time": "2024-02-20 12:29:37.345687",
# # 		"attendance_type": "Automated",
# # 		"center": "Dadar",
# # 		"youth_master": "YTH-23-00000489",
# # 		"unique_id": "2300000489",
# # 		"department": null,
# # 		"area": null,
# # 		"serial_number": null,
# # 		"device_name": null,
# # 		"actual_temperature": 0
# # 	},
# # 	{
# # 		"youth_id": "2300000490",
# # 		"first_name": "amitabh",
# # 		"date": "2024-02-20",
# # 		"in_time": "9:00:00",
# # 		"out_time": "23:00:00",
# # 		"upload_time": "2024-02-20 12:28:28.287596",
# # 		"attendance_type": "Automated",
# # 		"center": "Dadar",
# # 		"youth_master": "YTH-23-00000490",
# # 		"unique_id": "2300000490",
# # 		"department": null,
# # 		"area": null,
# # 		"serial_number": null,
# # 		"device_name": null,
# # 		"actual_temperature": 0
# # 	},

# # ]

# # Schedule the job to run every day at 10:30 PM
# # schedule.every().day.at("22:30").do(lambda: job(dummy_data))
# # schedule.every().day.at(db['time']).do(lambda: job(dummy_data))

# # while True:
# # 	schedule.run_pending()
# # 	time.sleep(1)


# input()