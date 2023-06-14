import fire
from bcy_scraper.gdl_runner import GdlRunner
from bcy_scraper.folder_handler import FolderHandler
from tqdm.auto import tqdm

from glob import glob


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
        runner.run_gallery_dl(handle, download_dir)

        # Find the actual directory
        actual_directory = glob(f"{download_dir}/{handle}###*")
        if actual_directory:
            # Process the first matching directory
            handler.process(actual_directory[0])
        else:
            print(f"No directory starting with {handle} was found in {download_dir}")


if __name__ == '__main__':
    fire.Fire(main)