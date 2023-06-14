import subprocess
from pathlib import Path

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



if __name__ == "__main__":
    file_path = "./assets/bcy_demo_handles"
    demo_download_dir = r"D:\Andrew\Pictures\==Train\download_bench"
    downloader = GdlRunner()
