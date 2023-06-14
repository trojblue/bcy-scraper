import subprocess
from pathlib import Path
from tqdm.auto import tqdm
import csv
import datetime
from typing import Optional
from utils.logger import PipelineLogger


class GdlRunner:
    def __init__(
        self, check_interval_days: int = 3, dst_dir: Optional[str] = None, logger: Optional[PipelineLogger] = None
    ):
        self.check_interval = datetime.timedelta(days=check_interval_days)
        self.dst_dir = dst_dir
        self.logger = logger or PipelineLogger("gallery_dl")

        # filters: python语法, 变量名见和图片一起保存的json
        self.filters = {
            "width": ">= 512",
            "height": ">= 512",
            "extension": "not in ('mp4')",
            # date: https://github.com/mikf/gallery-dl/discussions/3612
            # "date": "> datetime(2019, 1, 1)",
            # "favorite_count": "> 3",
        }

    @staticmethod
    def read_handles(file_path: str) -> list:
        with open(file_path, "r", encoding="utf-8") as f:
            return [line.strip() for line in f.readlines()]

    def run_gallery_dl(self, handle: str, dst_dir: Optional[str] = None):
        """
        使用gallery-dl下载*单个*半次元用户的图片到dst_dir
        """
        url = f"https://bcy.net/u/{handle}"
        flags = "--mtime-from-date --write-metadata --write-info-json"

        # Format filters into a command string
        filter_command = " and ".join(f"{k} {v}" for k, v in self.filters.items())
        filter_command = f'--filter "{filter_command}"'

        # Combine everything into the final command
        command = f"gallery-dl {url} {flags} {filter_command}"

        if dst_dir and not self.dst_dir:
            command += f" --dest {dst_dir}"
        else:
            command += f" --dest {self.dst_dir}"

        self.logger.info("gallery-dl: running command: " + command)

        # Run gallery-dl
        subprocess.run(command, shell=True)

        self.logger.info(f"gallery-dl: finished for {handle}")

    def check_and_update_csv(self, txt_path: str, csv_path: str = None):
        """
        从txt文件(每行一个handle)中读取handle, 如果检查需要下载则用gallery_dl
        :param txt_path: txt文件路径
        :param csv_path: csv文件路径 (留空则创建新的)
        """
        if csv_path is None:
            csv_path = txt_path.replace(".txt", ".csv")

        # Try to read the csv file to get the last checked times
        try:
            with open(csv_path, "r") as csvfile:
                reader = csv.DictReader(csvfile)
                last_checked = {
                    row["twitter_handle"]: datetime.datetime.strptime(row["last_checked"], "%Y-%m-%d %H:%M:%S.%f")
                    for row in reader
                }
        except FileNotFoundError:
            # If the csv file does not exist, assume all handles need to be checked
            last_checked = {}

        # Read the handles from the txt file
        handles = self.read_handles(txt_path)
        progress_bar = tqdm(handles, unit="user")
        now = datetime.datetime.now()

        for handle in progress_bar:
            # If the check interval for the handle has passed, download images for it
            last_checked_time = last_checked.get(handle)
            if last_checked_time is None or now - last_checked_time > self.check_interval:
                progress_bar.set_description(f"Downloading media for {handle}")
                self.run_gallery_dl(handle, self.dst_dir)
                last_checked[handle] = now

                # Update the CSV every 5 minutes
                if (datetime.datetime.now() - now).total_seconds() > 5 * 60:
                    print("Updating CSV...")
                    self.update_csv(csv_path, last_checked)
                    now = datetime.datetime.now()

        # Save the final state
        self.update_csv(csv_path, last_checked)

    @staticmethod
    def update_csv(csv_path: str, last_checked: dict):
        with open(csv_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)

            # Write column names
            writer.writerow(["twitter_handle", "last_checked"])

            for handle, checked_time in last_checked.items():
                writer.writerow([handle, checked_time])


if __name__ == "__main__":
    file_path = "./assets/bcy_demo_handles"
    demo_download_dir = r"D:\Andrew\Pictures\==Train\download_bench"
    downloader = GdlRunner()
    downloader.check_and_update_csv(file_path.replace('.txt', '.csv'), file_path)
