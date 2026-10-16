import csv
import json

# função que transforma o csv dos dados tratados em json
def csv_to_json(csv_file, json_file):

    data = []

    with open(csv_file, encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            data.append(row)
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

csv_to_json('clean_data.csv', 'formated-data.json')
