#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun  7 17:44:01 2020

@author: sabkhalid
"""

from bs4 import BeautifulSoup
import urllib.request as urllib2
import re

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

def text_urls(urls):
    output_file = open("output_urls.txt", 'w')
    for url in urls:
        output_file.write(str(url)+'\n')
    return (output_file)

text_urls(all_urls)   


#testing
#import wget
#z = all_urls[1]
#wget.download(z, '/Users/sabkhalid/desktop/Committe_project')
