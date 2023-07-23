def dedupe_file(file_path):
    with open(file_path, 'r') as f:
        lines = f.readlines()

    # Remove duplicates by converting the list to a set, then convert back to a list
    unique_lines = list(set(lines))

    with open(r"D:\Andrew\Downloads\bcy_jsons\cos_item_ids_unique.txt", 'w') as f:
        f.writelines(unique_lines)



dedupe_file(r"D:\Andrew\Downloads\bcy_jsons\cos_item_ids.txt")