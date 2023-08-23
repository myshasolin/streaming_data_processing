#!/usr/bin/python3

import json
import csv
import os
import sys

def get_json(csv_file_path):
    csvfile = open(csv_file_path, 'r')
    csv_dir, csv_filename = os.path.split(csv_file_path)
    jsonfile_path = os.path.join(csv_dir, csv_filename.replace('.csv', '.json'))
    jsonfile = open(jsonfile_path, 'w')

    reader = csv.reader(csvfile)
    header = next(reader)
    for row in reader:
        json_data = {}
        for i in range(len(header)):
            try:
                value = int(row[i].strip())
            except ValueError:
                try:
                    value = float(row[i].strip())
                except ValueError:
                    value = row[i].strip()
            json_data[header[i].strip()] = value
        json.dump(json_data, jsonfile)
        jsonfile.write('\n')
    
    csvfile.close()
    jsonfile.close()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Должно быть два аргумента: python3 csv_to_json.py <csv_file_path>')
        sys.exit(1)
    if len(sys.argv) > 2:
        print('Используются первые два аргумента, остальные игнорируются')
    
    csv_file_path = sys.argv[1]
    get_json(csv_file_path)
    
