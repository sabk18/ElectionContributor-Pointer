#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 23 20:57:18 2020

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

from sqlalchemy import create_engine
import sqlalchemy
import boto3
import s3fs

#from fast_to_sql import fast_to_sql as fts
import pymysql
import sqlalchemy
import sqlalchemy.types as sqltypes
from sqlalchemy import create_engine

##########################
# Load env file
#os.chdir('/home/ubuntu/HOME')
#from dotenv import load_dotenv
#dotenv_path = '/home/ubuntu/HOME' + '/staging.env'
#load_dotenv(dotenv_path)
#locals().update(os.environ)
##########################


def ZipCodeFile(df):
    df.to_csv("ZipCode_Nonvalid.csv", mode='a')

def ProcessFiles(df):

    new_df =df[0].str.split('|', expand=True)
    new_df.columns =['Committee_ID','Amendment_Indic','Report_Type','Election_Type','Image_Number','Transaction_Type',
                     'Entity_Type','Name','City','State','ZipCode','Employer','Occupation','Transaction_Date',
                     'Transaction_Amount','FEC_ID','Transaction_ID','Report_ID','Memo_Code','Memo_Txt','SUB_ID']
    #replce blank spaces with NaN
    new_df= new_df.replace(r'^\s*$', np.NaN, regex=True)

    #how to drop rows with Nan
    new_df = new_df.dropna(how='all')

    #reset index:
    new_df=new_df.reset_index(drop=True)

    new_df['ZipCode'] =new_df['ZipCode'].astype('str')
    mask_filter = (new_df['ZipCode'].str.len() != 5)
    Notvalid_zipcode = new_df.loc[mask_filter]

    ZipCodeFile(Notvalid_zipcode)


    #fetch out data with zip coes not equal to 5
    mask = (new_df['ZipCode'].str.len() == 5)
    new_df = new_df.loc[mask]

    #replace NaN with 0
    new_df=new_df.fillna(0)

    #change datatypes of columns
    new_df['Transaction_Type']=new_df['Transaction_Type'].astype(str)

    #change date string to datetime
    new_df['Transaction_Date']=pd.to_datetime(new_df['Transaction_Date'], format ="%m%d%Y",errors='coerce')
    new_df['Transaction_Date']=new_df['Transaction_Date'].dt.strftime('%Y-%m-%d')
    new_df['Transaction_Date']= new_df['Transaction_Date'].astype('datetime64[ns]')

    #new_df[['Last_Name','First_Name']]=new_df.Name.str.split(',', expand=True)
    #new_df.drop('Name', axis=1, inplace=True)


    return new_df
