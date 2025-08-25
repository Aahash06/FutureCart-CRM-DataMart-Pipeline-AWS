import os
import json
import random
from datetime import datetime, timedelta

# CONFIGURABLE PARAMETERS
NUM_DAYS = 10
CASES_PER_DAY = 100
SURVEYS_PER_DAY = 80  # must be ≤ CASES_PER_DAY
OUTPUT_DIR = "historical_data_jsonl"
def random_case_no():
    return str(random.randint(600000, 700000))
def random_timestamp(day_offset):
    date = datetime.now() - timedelta(days=day_offset)
    return date.strftime("%Y-%m-%d %H:%M:%S")
def generate_case(case_no, day_offset):
    return {
        "status": random.choice(["Open", "Closed"]),
        "category": f"CAT{random.randint(1,5)}",
        "sub_category": f"SCAT{random.randint(1,20)}",
        "last_modified_timestamp": random_timestamp(day_offset),
        "case_no": case_no,
        "create_timestamp": random_timestamp(day_offset),
        "created_employee_key": str(random.randint(200000, 300000)),
        "call_center_id": f"C-{random.randint(100,120)}",
        "product_code": str(random.randint(9000000, 9999999)),
        "country_cd": random.choice(["IN", "US", "BR", "DE", "AU"]),
        "communication_mode": random.choice(["Email", "Call", "Chat"])
    }

def generate_survey(case_no, day_offset):
    return {
        "survey_id": f"S-{random.randint(500000, 599999)}",
        "case_no": case_no,
        "survey_timestamp": random_timestamp(day_offset),
        "Q1": random.randint(1, 10),
        "Q2": random.randint(1, 10),
        "Q3": random.randint(1, 10),
        "Q4": random.choice(["Y", "N"]),
        "Q5": random.randint(1, 10)
    }

def ensure_dirs():
    os.makedirs(f"{OUTPUT_DIR}/cases", exist_ok=True)
    os.makedirs(f"{OUTPUT_DIR}/surveys", exist_ok=True)

def main():
    ensure_dirs()
    for day in range(1, NUM_DAYS + 1):
        case_nos = [random_case_no() for _ in range(CASES_PER_DAY)]
        cases = [generate_case(cn, day) for cn in case_nos]
        surveys = [generate_survey(cn, day) for cn in random.sample(case_nos, SURVEYS_PER_DAY)]

        # Write case data as JSON Lines
        with open(f"{OUTPUT_DIR}/cases/case_data_day{day}.json", "w") as f:
            for record in cases:
                f.write(json.dumps(record) + "\n")

        # Write survey data as JSON Lines
        with open(f"{OUTPUT_DIR}/surveys/survey_data_day{day}.json", "w") as f:
            for record in surveys:
                f.write(json.dumps(record) + "\n")

    print(f"✅ JSON Lines generated for {NUM_DAYS} days in '{OUTPUT_DIR}/'")

if __name__ == "__main__":
    main()
