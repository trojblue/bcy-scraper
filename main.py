import fire
from bcy_scraper.gdl_runner import GdlRunner

def main(file_path:str, download_dir:str="bcy"):
    """
    :param file_path: the txt file path
    :param download_dir: dir to save responses
    :return:
    """
    runner = GdlRunner()

    handles = runner.read_handles(file_path)
    for handle in handles:
        runner.run_gallery_dl(handle, download_dir)



if __name__ == '__main__':
    fire.Fire(main)