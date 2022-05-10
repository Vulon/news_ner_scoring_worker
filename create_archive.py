import nltk
import os

import shutil


import tempfile

with tempfile.TemporaryDirectory() as temp_folder:
    # nltk_folder = os.path.join(temp_folder, "NLTK")
    # os.mkdir(nltk_folder)
    # nltk.download('punkt', download_dir=nltk_folder)
    files_to_copy = [
        "config.json",
        "config.py",
        "create_archive.py",
        "main.py",
        "requirements.txt"
    ]
    folders_to_copy = [
        "big_querry",
        "utils",
        "exceptions"
    ]
    for file_name in files_to_copy:
        print("Copying file", file_name)
        shutil.copyfile(file_name, os.path.join(temp_folder, file_name))
    for folder_name in folders_to_copy:
        shutil.copytree(folder_name, os.path.join(temp_folder, folder_name))
    shutil.make_archive("news_ner_scoring", 'zip', temp_folder)
