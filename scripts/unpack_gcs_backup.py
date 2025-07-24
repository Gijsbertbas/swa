from datetime import datetime
import json
from pathlib import Path
import sys
from tqdm import tqdm
from uuid import UUID

TABLES = [
    # 'admin_dashboard_configurations',
    # 'challenges',
    # 'client_enabled_features',
    # 'client_groups',
    # 'clients',
    # 'comparison_monthly_usage_data',
    # 'content_keys',
    'daily_usage_data',
    # 'deleted_households',
    # 'ean_tool_logs',
    # 'electricity_comparison_groups',
    # 'external_house_dataset',
    # 'faq_categories',
    # 'faqs',
    # 'gas_comparison_groups',
    # 'hourly_usage_data',
    # 'household_comparison_group',
    # 'household_details',
    # 'household_energy_contracts',
    # 'household_house_details',
    # 'household_requests',
    # 'households',
    # 'households_external_datasets',
    # 'journey_stops',
    # 'monthly_usage_data',
    # 'network_operators',
    # 'notification_messages',
    # 'notification_settings',
    # 'notifications',
    # 'p4_day_data',
    # 'p4_errors',
    'p4_hour_data',
    # 'p4_month_data',
    # 'quiz_answers',
    # 'quiz_questions',
    # 'quizes',
    # 'table_configurations',
    # 'table_of_rights',
    # 'table_of_rights_old',
    # 'tips',
    # 'user_challenge_statuses',
    # 'user_devices',
    # 'user_journeys',
    # 'user_quiz_answers',
    # 'user_quizes',
    # 'user_tip_statuses',
    # 'your_excel_table_name'
]


def is_valid_uuid(uuid_to_test):
    try:
        uuid_obj = UUID(uuid_to_test)
    except ValueError:
        return False
    return str(uuid_obj) == uuid_to_test


def transform(record: dict, out_path: str):
    if 'daily_usage_data' in out_path:
        return {
            'household_id': record['household_id'],
            'activation_code': record['household_activation_code'],
            'date': record['date'],
            'type': record['type'],
            'usage': record['usage'],
        }
    elif 'p4_hour_data' in out_path:
        if 'datetime' not in record:
            print(f'Incorrect data format for {out_path}, missing datetime field')
            return None
        
        local_time = datetime.fromisoformat(record['datetime'])
        houseid = record['houseID']
        if not is_valid_uuid(houseid):
            record['house_id'] = houseid
        else:
            record['household_id'] = houseid

        return {
            'household_id': record.get('household_id'),
            'house_id': record.get('house_id'),
            'datetime': record['datetime'],
            'date': local_time.date().isoformat(),
            'time': local_time.time().isoformat(),
            'backfeed': record['backfeedMeasurement']['meter'] if 'backfeedMeasurement' in record else None,
            'electricity': record['electricityMeasurement']['meter'] if 'electricityMeasurement' in record else None,
            'gas': record['gasMeasurement']['meter'] if 'gasMeasurement' in record else None,
        }
    else:
        print(f'Uknown record: {out_path}')


def parse(file_name: str, out_path: str):

    output_path = Path(out_path)
    output_path.mkdir(parents=True, exist_ok=True)

    output_file = output_path / file_name.name

    records = json.load(open(file_name, 'r'))
    if records:
        with open(output_file, 'w') as f:
            for record in records:
                record = transform(record, out_path)
                if record:
                    f.write(json.dumps(record) + '\n')
    else:
        # not a backup file
        return


def unpack(root_path: str):
    root = Path(root_path)

    for folder_path in tqdm(root.iterdir(), desc="Unpacking GCS backup files"):
        if folder_path.is_file():
            for table in TABLES:
                if table in folder_path.name:
                    output_path = 'extracted/' + table
                    parse(folder_path, output_path)
                    break
                

if __name__ == "__main__":
    root = "./gcs"
    if len(sys.argv) > 1:
        root = sys.argv[1]
    unpack(root)
