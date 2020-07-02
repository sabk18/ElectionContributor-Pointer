#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 23 21:12:52 2020

@author: sabkhalid
"""


from bs4 import BeautifulSoup
import urllib.request as urllib2
import re

import os
import os.path
import glob
import sys
import csv
import shutil
sys.path.insert(0, os.environ["HOME"])
#os.chdir('/Users/sabkhalid/airflow/dags/Python_Modules')
os.chdir('/home/ubuntu/Python_Modules')
#from Python_Modules.ExtractingUrls import *
from Python_Modules.s3Files import *
from Python_Modules.Cleaning import *
from Python_Modules.database import *
from Python_Modules.fileconverter import *


import pandas as pd
import numpy as np
import logging
from io import StringIO, BytesIO
import regex
#import mysql.connector
from hurry.filesize import size

import time
from datetime import datetime, timedelta, date
from time import time
import zipfile
from array import *


##########################
from sqlalchemy import create_engine
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

import logging
import boto3
from botocore.exceptions import ClientError
#########################3




    
today = datetime.now()
today_str = today.strftime('%Y-%m-%d')
today_year = today.strftime('%Y')

html_page = urllib2.urlopen("https://www.fec.gov/data/browse-data/?tab=bulk-data")

def Parse_web(html_page):
    soup = BeautifulSoup(html_page, "lxml")
    x= soup.find_all(class_="list--flat-bordered")[21]
    links = [a["href"] for a in x.select("a[href]")]

    all_urls =[]

    for link in links:
        if '20' in link:
            url = "https://www.fec.gov" + link
            all_urls.append(url)
        
    return all_urls
    
all_urls = Parse_web(html_page)

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

def text_urls(urls):
    today = datetime.now()
    today_str = today.strftime('%Y-%m-%d')
    Createfolder("url_folder")
    output_file = open("url_folder/output_urls_{}.txt".format(today_str), 'w')
    for url in urls:
        output_file.write(str(url)+'\n')
        
def read_text(file):
    file = open(file).readlines()
    return file

        
def rename_files(folder):
        #fetch these files, change their names
        #count = 0
        new_filenames =[]
        for count, filename in enumerate(os.listdir(folder)):
            print (filename)
            src = folder + filename
        #src = file_path
            new_name = filename.replace('.txt', '_{}_{}.txt'.format(str(count),today_str))
            os.rename(src, folder + "/" + new_name)
            new_filenames.append(new_name)
        return new_filenames
    
def timestamp_files(filename):
    new_name = filename.replace('.txt', '_{}.txt'.format(today_str))
    return new_name

def appending_compfile(filenames):
    output_file = open("Fetched.txt" , 'a+')
    for names in filenames:
        output_file.write(names+ '\n')
        
        
def array_list(array_num):
    num_list = array_num.tolist()
    return num_list


os.chdir('/home/ubuntu/')
#save all urls into a txt file
text_urls(all_urls)

path = '/home/ubuntu/url_folder'

files = os.listdir(path)

url_list = []
for file in files:
    file_htmls = read_text(path + "/" + file)
    for htmls in file_htmls:
        #####################
        #url_list.append(htmls)
        #####################
        if today_year in htmls:
            print ("yes exists")
            url = htmls
            url_list.append(url)
            
count = 0
#now that we have the current url for this year we can unzip contents of that year
for url in url_list:
    #url ='https://www.fec.gov/files/bulk-downloads/2010/indiv10.zip'
    count += 1
    content = urllib2.urlopen(url)
    
    folder = path + "/" + "Unzipped_Files"
    Createfolder(folder)
    
    #compare with a comp file to get names of new files to downlaod
    file_exists = os.path.isfile('./Fetched.txt')
    if not file_exists:
        zip_file = zipfile.ZipFile(BytesIO(content.read()))
        zip_file.extractall(folder)
    else:
        zip_file = zipfile.ZipFile(BytesIO(content.read()))
        file_names = zip_file.namelist()
        in_list = []
        for names in file_names:
#                file_name = names.replace('.txt', '_{}.txt'.format(today_str))
            in_liist.append(names)
        
        comparing_file = read_text("Fetched.txt")
        new_urls = np.setdiff1d(in_list,comparing_file)
        new_urls = array_list(new_urls)
#            orig_urls =[]
#            for i in new_urls:
#                orig_name = i.replace('_{}.txt'.format(today_str),'.txt')
#                orig_urls.append(orig_name)
            
        for url in new_urls:
            zip_file.extract(url, folder)
  
    if len(os.listdir(folder + "/")) == 0:
        print ("No new files found in Unzipped Folder")
        shutil.rmtree(folder)
        print ("Folder has been removed successfully")
    else:
        #move the files inside the zipped folder to S3 Bucket
        s3 = boto3.resource('s3')
        indiv_file_names =[]
        for filename in os.listdir(folder+ "/"):
            if filename == '.DS_Store':
                pass
            elif filename == 'by_date':
                new_path = folder + "/by_date/"
                for file in os.listdir(new_path):
                    indiv_file_names.append(file)
                    file_path = new_path + file
                    bucket = "campaign-project"
                    try:
                        response = s3.meta.client.upload_file(file_path, bucket, 'by_date_files/{}'.format(timestamp_files(file)))
                        print ("file {} has been updates to the by_date folder in bucket {} on date {}".format(file,bucket, today_str))
                    except ClientError as e:
                        logging.error(e)
            else:
                #new_name = filename + "_{}_{}".format(str(count),today_str)
                #indiv_file_names.append(new_name)
                file_path = folder + "/"  + filename
                print ("my file_path is: {}".format(file_path))
                src = file_path
                new_name = filename.replace('.txt', '_{}_{}.txt'.format(str(count),today_year))
                os.rename(src, folder + "/" + new_name)
                #indiv_file_names.append(new_name)
                bucket = "campaign-project"
                try:
                    response = s3.meta.client.upload_file(folder + "/" + new_name, bucket, 'Web_files(raw)/{}'.format(new_name))
                    print ("file {} has been updated to the web_files(raw) folder in bucket {} on date {}".format(new_name,bucket, today_str))
                except ClientError as e:
                    logging.error(e)
                
        #now store the new file names into a Fetched txt for future files to copare names with
        appending_compfile(indiv_file_names)
    
    
        #remove the folder : Unzipped_files
        #os.remove(path  + "/Unzipped_Files")
        shutil.rmtree(path + '/Unzipped_Files')
        print ("Folder has been removed successfully")

 

path = '/home/ubuntu/url_folder'

engine = create_engine('mysql+pymysql://' + mysql_user + ':'+ mysql_password + '@' + host + ':' + str(port) +'/' + db, echo=False)
conn = engine.connect()

#Bucket_2 ='indivi-files'
Bucket ="campaign-project"
s3folder = "by_date_files"
files_list =s3SubFolder(Bucket,s3folder)
#files_list = fetch_s3files(Bucket)
info_list =[]
#files_list = files_list_2[0:2]
for file_ in files_list:
    print (file_)
    #df= pd.read_csv('s3://%s/%s'%(Bucket , file_),header=None, sep='\t', error_bad_lines=False, encoding='latin-1')
    df= pd.read_csv('s3://%s/%s/%s'%(Bucket, s3folder, file_),header=None, sep='\t', error_bad_lines=False, encoding='latin-1')
    t_1 = time()
    new_df = ProcessFiles(df)
    t_2 =time()
    pandas_t = (t_2 - t_1)
    print ("Dataset for the file {} has been processed in {} seconds".format(file_,pandas_t))
    #FileConvert(new_df, file_)
    print ("File {} has been saved ".format(file_))
    start_time = time()
    ToSQL(new_df, engine)
    end_time = time()
    sql_t = (end_time - start_time)
    print ("File {} loaded successfully in MySQL Database in {} seconds".format(file_,sql_t))

#file_size = filesize(file_)
    info_list.append([file_, pandas_t, sql_t])
    Infofile(path,info_list)

conn.close()




####################################################################################################################################


