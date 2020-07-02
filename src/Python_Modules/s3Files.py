#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 23 20:53:40 2020

@author: sabkhalid
"""
import os
import sys
import csv
sys.path.insert(0, os.environ["HOME"])

import pandas as pd
import numpy as np


import time
from datetime import datetime, timedelta, date
from time import time


import boto3
import s3fs
##########################


##########################


def fetch_s3files(bucket):
    #fetch files inside the bucket 
    s3 = boto3.resource('s3')
    target_bucket= s3.Bucket(bucket)

    file_list =[]
    for s3_file in target_bucket.objects.all():
        file = s3_file.key
        file_list.append(file)
        
    return file_list

#Bucket = 'indiv-raw-data'


####### use below ##########
#Bucket_2 ='indivi-files'


def s3SubFolder(bucket,s3folder):
    s3 = boto3.client('s3')
    indivfile_list =[]
    for obj in s3.list_objects_v2(Bucket= bucket)['Contents']:
        if s3folder in obj['Key']:
            FileName = obj['Key'].split('/')
            FileName = FileName[1]
            indivfile_list.append(FileName)
        else:
            pass
    unwanted_list =['','itcont_2014_invalid_dates.txt','itcont_2012_invalid_dates.txt','itcont_2016_invalid_dates.txt','itcont_2018_invalid_dates.txt','itcont_2020_invalid_dates_2020-07-02.txt']
    new_list = [x for x in indivfile_list if x not in unwanted_list]


    return new_list


#files_list =s3SubFolder(Bucket_2)


