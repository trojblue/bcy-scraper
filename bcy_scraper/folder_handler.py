import os
import json
import tarfile
import shutil
import pandas as pd
from typing import List, Dict
from glob import glob


class FolderHandler:
    def __init__(self, user_info_dir: str):
        self.user_infos = user_info_dir
        if not os.path.exists(user_info_dir):
            os.makedirs(user_info_dir)

    def process(self, directory: str) -> None:
        if not os.path.exists(directory):
            raise FileNotFoundError(f"The directory {directory} does not exist.")

        df = self._load_and_combine_json(directory)
        if df.empty:
            print(f"No JSON data found in {directory}.")
            return

        self._save_as_parquet(df, directory)
        self._copy_info_json(directory)
        self._rename_associated_jsons(directory)
        self._compress_and_delete_directory(directory)


    @staticmethod
    def parse_json_to_df(json_dict: Dict) -> pd.DataFrame:
        """
        >>> handler = FolderHandler('user_infos')
        >>> df = handler.parse_json_to_df(json_dict)
        >>> df.to_parquet('output.parquet')
        :return:
        """
        data = {
            'user_id': json_dict['user']['id'],
            'name': json_dict['user']['name'],
            # 'utags': "###".join(json_dict['user']['utags']),
            'post_id': json_dict['post']['id'],
            'tags': "###".join(json_dict['post']['tags']),
            'date': json_dict['post']['date'],
            'parody': json_dict['post']['parody'],
            'content': json_dict['post']['content'],
            'likes': json_dict['post']['likes'],
            'shares': json_dict['post']['shares'],
            'replies': json_dict['post']['replies'],
            'type': json_dict['post']['image_list'][0]['type'],
            'mid': json_dict['post']['image_list'][0]['mid'],
            'w': json_dict['post']['image_list'][0]['w'],
            'h': json_dict['post']['image_list'][0]['h'],
            'original_path': json_dict['post']['image_list'][0]['original_path'],
            'visible_level': json_dict['post']['image_list'][0]['visible_level'],
            'format': json_dict['post']['image_list'][0]['format'],
            'category': json_dict['category'],
            'subcategory': json_dict['subcategory'],
            'num': json_dict['num'],
            'id': json_dict['id'],
            'width': json_dict['width'],
            'height': json_dict['height'],
            'filename': json_dict['filename'],
            'extension': json_dict['extension'],
            'filter': json_dict['filter'],
            'orig_path': json_dict['orig_path']
        }

        return pd.DataFrame([data])
    def _load_and_combine_json(self, directory: str) -> pd.DataFrame:
        json_files = glob(os.path.join(directory, "*.json"))
        json_files = [file for file in json_files if "info.json" not in file]

        data_frames = []
        for file in json_files:
            with open(file, 'r') as f:
                data = json.load(f)
                df = self.parse_json_to_df(data)  # Your original function
                data_frames.append(df)

        if not data_frames:
            return pd.DataFrame()  # Empty DataFrame

        combined_df = pd.concat(data_frames, ignore_index=True)
        return combined_df

    def _save_as_parquet(self, df: pd.DataFrame, directory: str) -> None:
        parquet_path = f"{directory}.parquet"
        df.to_parquet(parquet_path)

    def _copy_info_json(self, directory: str) -> None:
        src = os.path.join(directory, "info.json")
        if not os.path.isfile(src):
            print(f"info.json does not exist in {directory}.")
            return

        directory_name = os.path.basename(directory)
        dest = os.path.join(self.user_infos, f"{directory_name}_info.json")
        shutil.copy2(src, dest)

    def _rename_associated_jsons(self, directory: str) -> None:
        all_files = glob(os.path.join(directory, "*"))
        for file in all_files:
            basename = os.path.basename(file)
            json_file = f"{file}.json"
            if basename.endswith(".jpg") and os.path.isfile(json_file):
                try:
                    os.rename(json_file, file.replace(".jpg", ".json"))
                except Exception as e:
                    print(f"Failed to rename {json_file}: {e}")

    def _compress_and_delete_directory(self, directory: str) -> None:
        try:
            with tarfile.open(f"{directory}.tar", "w") as tar:
                tar.add(directory, arcname=os.path.basename(directory))
        except Exception as e:
            print(f"Failed to compress {directory}: {e}")
            return

        try:
            shutil.rmtree(directory)
        except Exception as e:
            print(f"Failed to delete {directory}: {e}")

if __name__ == '__main__':
    processor = FolderHandler(user_info_dir="./user_infos")
    processor.process("/home/studio-lab-user/dev/gdld/gallery-dl/1037581441053848###木甘牌甘木_2")