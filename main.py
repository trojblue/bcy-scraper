import fire
from bcy_scraper.gdl_runner import GdlRunner



def main(file_path:str, download_dir):
    demo_download_dir = r"D:\Andrew\Pictures\==Train\download_bench"
    runner = GdlRunner()
    runner.check_and_update_csv(file_path.replace('.txt', '.csv'), file_path)


if __name__ == '__main__':
    fire.Fire(main)