import csv
import json

def csv_to_json(csv_filepath, json_filepath):
    data = []
    with open(csv_filepath, 'r') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for row in csv_reader:
            data.append(row)

    with open(json_filepath, 'w') as jsonfile:
        json.dump(data, jsonfile, indent=4)

# Example usage:
csv_to_json('./data/sales_suppliers.csv', './data/sales_suppliers.json')