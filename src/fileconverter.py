#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 23 21:32:38 2020

@author: sabkhalid
"""
import os
import sys
import csv
sys.path.insert(0, os.environ["HOME"])



import pandas as pd
import numpy as np
import logging
from io import StringIO
import regex
#import mysql.connector
from hurry.filesize import size

import time
from datetime import datetime, timedelta, date
from time import time

def Createfolder(folder):
    mydir = folder
    #check if folder exists
    chk_folder = os.path.isdir(mydir)
    #if not then create it
    if not chk_folder:
        os.makedirs(mydir)
        print ("created folder : ",mydir)
    else:
        print (mydir, "folder already exists.")
        
        
def FileConvert(df,file_name):
    today = datetime.now()
    today_str = today.strftime('%Y-%m-%d')
    Folder = "Saved_files"
    Createfolder(Folder)
    df.to_csv('{}/{}_'.format(Folder,file_name) + today_str +'.csv', index=False)


def Infofile(info_list):
    with open("Analyzer_file.csv", mode="w") as csv_file:
        csvwriter = csv.writer(csv_file)
        csvwriter.writerow([info_list])