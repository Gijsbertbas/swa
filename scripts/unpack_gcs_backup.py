import json
from pathlib import Path
import sys
from tqdm import tqdm

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

def transform(record: dict, out_path: str):
    if 'daily_usage_data' in out_path:
        return {
            'household_id': record['household_id'],
            'activation_code': record['household_activation_code'],
            'date': record['date'],
            'type': record['type'],
            'usage': record['usage'],
        }
    else:
        print(f'Uknown record: {out_path}')


def parse(file_name: str, out_path: str):

    output_path = Path(out_path)
    output_path.mkdir(parents=True, exist_ok=True)

    output_file = output_path / file_name.name

    records = json.load(open(file_name, 'r'))
    if records:  # and 'requestDatetime' in records[0]: 
        with open(output_file, 'w') as f:
            for record in records:
                record = transform(record, out_path)
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
