#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 23 21:13:45 2020

@author: sabkhalid
"""
import os
import sys
import csv
sys.path.insert(0, os.environ["HOME"])


from sqlalchemy import create_engine
#from fast_to_sql import fast_to_sql as fts
import pymysql
import sqlalchemy
import sqlalchemy.types as sqltypes
from sqlalchemy import create_engine


##########################
# Load env file
from dotenv import load_dotenv
dotenv_path = os.environ['HOME'] + '/staging.env'
load_dotenv(dotenv_path)
locals().update(os.environ)
##########################
# Load env file from ec2 instance:
#from dotenv import load_dotenv
#os.chdir('/home/ubuntu/HOME')
#dotenv_path = '/home/ubuntu/HOME' + '/staging.env'
#load_dotenv(dotenv_path)
#locals().update(os.environ)
##########################



def ToSQL(df):
    
    #save the dataframe to MySQL:
    df.to_sql(name ='Contribution_dataset_staging', con=engine, if_exists ='append', index=False, chunksize=50000,
              dtype={'Committee_ID': sqlalchemy.types.NVARCHAR(length=9),
                     'Amendment_Indic': sqlalchemy.types.NVARCHAR(length=1),
                     'Report_Type':sqlalchemy.types.NVARCHAR(length=3),
                     'Election_Type':sqlalchemy.types.NVARCHAR(length=5),
                     'Image_Number':sqlalchemy.types.NVARCHAR(length=20),
                     'Transaction_Type':sqlalchemy.types.NVARCHAR(length=3),
                     'Entity_Type':sqlalchemy.types.NVARCHAR(length=3),
                     'Name':sqlalchemy.types.NVARCHAR(length=200),
                     'City':sqlalchemy.types.NVARCHAR(length=30),
                     'State':sqlalchemy.types.NVARCHAR(length=2),
                     'ZipCode':sqlalchemy.types.NVARCHAR(length=9),
                     'Employer':sqlalchemy.types.NVARCHAR(length=38),
                     'Occupation':sqlalchemy.types.NVARCHAR(length=38),
                     'Transaction_Date':sqlalchemy.types.DATE,
                     'Transaction_Amount':sqlalchemy.DECIMAL,
                     'FEC_ID':sqlalchemy.types.NVARCHAR(length=9),
                     'Transaction_ID':sqlalchemy.types.NVARCHAR(length=32),
                     'Report_ID':sqlalchemy.types.NVARCHAR(length=22),
                     'Memo_Code':sqlalchemy.types.NVARCHAR(length=1),
                     'Memo_Txt':sqlalchemy.types.NVARCHAR(length=100),
                     'SUB_ID':sqlalchemy.types.NVARCHAR(length=19),
                     }
              )
    
    

