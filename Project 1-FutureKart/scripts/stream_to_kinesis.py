import boto3
import random
import time
import calendar
import json
from datetime import datetime, timedelta

# Initialize Kinesis client
kinesis = boto3.client('kinesis', region_name='ap-south-1')
stream_name = 'aahash-stream'

# Read case data
with open('realtimedata/000000_0', 'r') as case_data_obj:
    all_case_data_lines = case_data_obj.readlines()

open_case_time_diff_mins = [40, 50, 60]
closed_case_time_diff_mins = [5, 10, 20, 30]
number_of_cases_counts = [1, 2, 3, 4, 5, 6]
scores = list(range(1, 11))
answer = ["Y", "N"]
survey_id_start = 500000
category = 'CAT3'
sub_categorys = ['SCAT8', 'SCAT9', 'SCAT10', 'SCAT11', 'SCAT12', 'SCAT13', 'SCAT14', 'SCAT15', 'SCAT16']

total_cases = len(all_case_data_lines)
i = 900

while i <= (total_cases - 1):
    sub_category = random.choice(sub_categorys)
    number_of_cases = random.choice(number_of_cases_counts)
    cases = all_case_data_lines[i:i + number_of_cases]

    current_timestamp = datetime.now()
    case_created_ts = str(current_timestamp - timedelta(minutes=random.choice(open_case_time_diff_mins)))[:19]
    case_closed_ts = str(current_timestamp - timedelta(minutes=random.choice(closed_case_time_diff_mins)))[:19]
    survey_ts = str(current_timestamp)[:19]
    file_ts = calendar.timegm(time.gmtime())

    for j in cases:
        case_no, created_employee, call_center, status, category1, sub_category1, mode, country, product = j.strip().split(',')

        case_data = {
            "case_no": case_no,
            "created_employee_key": created_employee,
            "call_center_id": call_center,
            "status": "Closed" if i % 5 == 0 else status,
            "category": category,
            "sub_category": sub_category,
            "communication_mode": mode,
            "country_cd": country,
            "product_code": product,
            "last_modified_timestamp": case_closed_ts if i % 5 == 0 else case_created_ts,
            "create_timestamp": case_created_ts
        }

        # Send to Kinesis
        kinesis.put_record(
            StreamName=stream_name,
            Data=json.dumps(case_data),
            PartitionKey=case_no
        )
        print("Sent to Kinesis:", case_data)

        if i % 5 == 0:
            survey_data = {
                "survey_id": f"S-{survey_id_start}",
                "case_no": case_no,
                "survey_timestamp": survey_ts,
                "Q1": random.choice(scores),
                "Q2": random.choice(scores),
                "Q3": random.choice(scores),
                "Q4": random.choice(answer),
                "Q5": random.choice(scores)
            }
            kinesis.put_record(
                StreamName=stream_name,
                Data=json.dumps(survey_data),
                PartitionKey=case_no
            )
            print("Sent survey to Kinesis:", survey_data)
            survey_id_start += 1

    i += number_of_cases
    time.sleep(5)
