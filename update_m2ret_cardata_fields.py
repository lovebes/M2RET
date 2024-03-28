#!/usr/bin/python3
import sys
import re
import time
import os
import argparse
import subprocess
import traceback
from os.path import dirname, basename, join, exists, expanduser, isdir

# update_cardata_fields.py: https://gitlab.com/ktpanda/car_monitor/raw/master/update_cardata_fields.py
import update_cardata_fields as ud


UPDATE_FILES = [
    'bt_interface.cpp',
]

def main():
    p = argparse.ArgumentParser(description='')
    args = p.parse_args()

    changes = {}

    ud.add_struct_changes(changes)
    ud.add_m2ret_changes(changes)

    ud.do_updates(UPDATE_FILES, changes)

if __name__ == '__main__':
    main()
