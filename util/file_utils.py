# -*- coding: utf-8 -*-

import os


def current_path():
    return os.getcwd()

def create_dir_if_not_exists(path):
    if os.path.exists(path):
        return
    os.makedirs(path)

def join_path_filename(path, filename):
    return os.path.join(path, filename)
