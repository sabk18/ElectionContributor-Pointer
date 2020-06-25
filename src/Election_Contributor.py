#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 23 21:12:52 2020

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

#import python Modules:
from ExtractingUrls import *   
from s3Files import s3SubFolder, fetch_s3files
from Cleaning import *
from database import *
from fileconverter import *

##########################
from sqlalchemy import create_engine
#from fast_to_sql import fast_to_sql as fts
import pymysql
import sqlalchemy
import sqlalchemy.types as sqltypes
from sqlalchemy import create_engine

##########################
# Load env file by placing your env file in a folder called HOME:
from dotenv import load_dotenv
os.chdir('/home/ubuntu/HOME')
dotenv_path = '/home/ubuntu/HOME' + '/staging.env'
#dotenv_path = os.environ['HOME'] + '/staging.env'
load_dotenv(dotenv_path)
locals().update(os.environ)
##########################


def main():
    engine = create_engine('mysql+pymysql://' + mysql_user + ':'+ mysql_password + '@' + host + ':' + str(port) +'/' + db, echo=False)
    conn = engine.connect()
    
    Bucket_2 ='indivi-files'
    #Bucket = 'indiv-raw-data'
    files_list_2 =s3SubFolder(Bucket_2)
    #files_list = fetch_s3files(Bucket)
    info_list =[]

    for file_ in files_list_2:
        print (file_)
        #df= pd.read_csv('s3://%s/%s'%(Bucket , file_),header=None, sep='\t', error_bad_lines=False, encoding='latin-1')
        df= pd.read_csv('s3://%s/%s/%s'%(Bucket_2, 'Folder', file_),header=None, sep='\t', error_bad_lines=False, encoding='latin-1')
        t_1 = time()
        new_df = ProcessFiles(df)
        t_2 =time()
        pandas_t = (t_2 - t_1)
        print ("Dataset for the file {} has been processed in {} seconds".format(file_,pandas_t))
        FileConvert(new_df, file_)
        print ("File {} has been saved ".format(file_))
        start_time = time()
        ToSQL(new_df)
        end_time = time()
        sql_t = (end_time - start_time)
        print ("File {} loaded successfully in MySQL Database in {} seconds".format(file_,sql_t))
    
    #file_size = filesize(file_)
        info_list.append([file_, pandas_t, sql_t])
        Infofile(info_list)
        
    
    
    
    conn.close()
    
if __name__ == "__main__":
    main()





