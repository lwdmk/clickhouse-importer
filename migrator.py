import csv
import requests
import datetime
import json
from pprint import pprint

writer = csv.writer(open('converted.csv', 'w', newline=''), delimiter='|', lineterminator='\n')
with open('SELECT_FROM_analytics_track_event_JOIN.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        print(f'Processind record with id: {row[0]}')
        if (row[16] == ''):
            continue
        createdAt = datetime.datetime.strptime(row[16], '%Y-%m-%d %H:%M:%S')
        payload = json.loads(row[20]) if (row[20] != 'NULL') else {}
        print(f'Processind record with id: {row[16]}')
        data = {}
        for key in payload:
            if (payload[key] == False):
                data[key] = str(payload[key]).lower()
            elif isinstance(payload[key], list) or isinstance(payload[key], dict):
                data[key] = json.dumps(payload[key])
            else:
                data[key] = str(payload[key]).replace("'", "\'")

        event = {
            'type': row[1],
            'source': 'edu_track_event',
            'timestamp': int(createdAt.timestamp()),
            'context': {
                'browserName': row[4].replace("'", "`") if row[4] != 'NULL' else "null",
                'browserVersion': row[5].replace("'", "`") if row[5] != 'NULL' else "null", 
                'engineName': row[6].replace("'", "`") if row[6] != 'NULL' else "null", 
                'engineVersion': row[7].replace("'", "`") if row[7] != 'NULL' else "null", 
                'osName': row[8].replace("'", "`") if row[8] != 'NULL' else "null", 
                'osVersion': row[9].replace("'", "`") if row[9] != 'NULL' else "null", 
                'deviceModel': row[10].replace("'", "`") if row[10] != 'NULL' else "null", 
                'deviceType': row[11].replace("'", "`") if row[11] != 'NULL' else "null", 
                'deviceVendor': row[12].replace("'", "`") if row[12] != 'NULL' else "null", 
                'ipString': row[13].replace("'", "`") if row[13] != 'NULL' else "null", 
                'country_name': row[14].replace("'", "`") if row[14] != 'NULL' else "null", 
                'city_name': row[15].replace("'", "`") if row[15] != 'NULL' else "null", 
                'region_iso': row[19].replace("'", "`") if row[19] != 'NULL' else "null", 
                'region': row[18].replace("'", "`") if row[18] != 'NULL' else "null"
            },
            'data': data,
            'user': {
                'user_id': row[2],
                'createdAt': "null",
                'type': "null"
            }
        }
        csv_row = [
            event['timestamp'],
            event['type'],
            event['source'],
            list(event['data'].keys()),
            list(event['data'].values()),
            list(event['user'].keys()),
            list(event['user'].values()),
            list(event['context'].keys()),
            list(event['context'].values())
        ]
        writer.writerow(csv_row)
        # response = requests.post('https://event-analytics-backend.skyeng.ru/api/quarantine/events',
        #     json = event,
        #     headers = {
        #         'Content-Type': 'application/json',
        #         'Authorization': 'Bearer MTEwZDVhN2IzYTNjZTIxYTU4OTZkNmRjZmE0OWI5NjE'
        #     }
        # )
        # print(f'Result: {response}. {response.content}')
        print(f'Line {line_count}')
        line_count = line_count + 1

    print(f'Processed {line_count} lines.')