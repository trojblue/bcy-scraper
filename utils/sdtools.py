"""
远古sdtools的一些函数, 保留兼容性
"""
import os
from typing import *

__all_imgs_raw = (
    "jpg jpeg png bmp dds exif jp2 jpx pcx pnm ras gif tga tif tiff xbm xpm webp"
)
IMG_FILES = ["." + i.strip() for i in __all_imgs_raw.split(" ")]


def get_files_with_suffix(
    src_dir: str, suffix_list: List[str], recursive: bool = False
):
    """
    :param src_dir:
    :param suffix_list: ['.png', '.jpg', '.jpeg']
    :param recursive: 是否读取子目录
    :return: ['img_filename.jpg', 'filename2.jpg']
    """
    if recursive:  # go through all subdirectories
        filtered_files = []
        for root, dirs, files in os.walk(src_dir):
            for file in files:
                if file.endswith(tuple(suffix_list)):
                    filtered_files.append(os.path.join(root, file))

    else:  # current dir only
        files = os.listdir(src_dir)
        filtered_files = [f for f in files if f.endswith(tuple(suffix_list))]

    return filtered_files
