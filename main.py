import fire
from bcy_scraper.gdl_runner import GdlRunner
from bcy_scraper.folder_handler import FolderHandler
from tqdm.auto import tqdm
import os


def find_folder(handle, base_dir):
    handle = str(handle)

    for root, dirs, _ in os.walk(base_dir):
        for dir in dirs:
            # Split the directory name on ###
            parts = dir.split('###')

            # Check if the first part of the directory name is the handle
            if parts[0] == handle:
                return os.path.join(root, dir)

    return None


def main(file_path: str, download_dir: str = "bcy", user_info_dir: str = "user_info"):
    """
    :param file_path: the txt file path
    :param download_dir: dir to save responses
    :return:
    """
    runner = GdlRunner()
    handler = FolderHandler(user_info_dir=user_info_dir)

    handles = runner.read_handles(file_path)
    pbar = tqdm(handles)
    for handle in pbar:
        pbar.set_description(handle)
        # runner.run_gallery_dl(handle, download_dir)
        folder_name = find_folder(handle, download_dir)
        handler.process(folder_name)


if __name__ == '__main__':
    fire.Fire(main)