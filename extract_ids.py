import json
import os
from tqdm import tqdm

def extract_item_ids_from_dir(directory):
    item_ids = []
    json_files = [f for f in os.listdir(directory) if f.endswith(".json")]
    for filename in tqdm(json_files, desc="Processing files"):
        with open(os.path.join(directory, filename), 'r') as f:
            data = json.load(f)
        item_ids += [item['item_detail']['item_id'] 
                     for item in data['data']['top_list_item_info'] 
                     if 'item_detail' in item and 'item_id' in item['item_detail']]

    return item_ids

def write_ids_to_file(item_ids, file_path):
    with open(file_path, 'w') as f:
        for item_id in item_ids:
            f.write(f'{item_id}\n')

if __name__ == '__main__':
    item_ids = extract_item_ids_from_dir(r"D:\Andrew\Downloads\bcy_jsons\novel")
    write_ids_to_file(item_ids, r"D:\Andrew\Downloads\bcy_jsons\novel_item_ids.txt")
