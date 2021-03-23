#!/usr/bin/python
import os
import requests
import sys
from retrying import retry
import datetime

# Don't forget to remove your previous logs
# rm -rf ../logs/geocodage/*

#ADDOK_URL = 'http://localhost:9999/addok/search/csv/'
#ADDOK_URL = 'http://localhost:5000/search/csv'
ADDOK_URL = 'http://IP-ADDOK/addok/search/csv/'

def write_or_append_file(pathfile):
    
    filename = pathfile.split('/')[-1]
    folder = pathfile.split('/')[-2]

    if os.path.exists('../logs/geocodage/'+folder+'.txt'):
        append_write = 'a' # append if already exists
    else:
        append_write = 'w' # make a new file if not

    logfile = open('../logs/geocodage/'+folder+'.txt',append_write)
    logfile.write(filename+'\n')
    logfile.close()

def retry_if_io_error(exception):
    """Return True if we should retry (in this case when it's an IOError), False otherwise"""
    return isinstance(exception, KeyError)


#@retry(stop_max_attempt_number=7)
def geocode(folderpath_in, csvfile, folderpath_out):
    with open(folderpath_in+csvfile, 'rb') as f:
        filename, response = post_to_addok(folderpath_in+csvfile, f.read())
        write_response_to_disk(folderpath_out, csvfile, response)

def write_response_to_disk(folderpath_out, csvfile, response, chunk_size=1024):
    with open(folderpath_out+"result-"+csvfile, 'wb') as fd:
        for chunk in response.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


def post_to_addok(filename, filelike_object):
    files = {'data': (filename, filelike_object)}
    status = 500
    essai = 0
    while status == 500:
        essai = essai + 1
        response = requests.post(ADDOK_URL, files=files)
        status = response.status_code
        if((status == 500) & (essai == 100)):
            status = 333
            print("too much retry in "+filename)
            write_or_append_file(filename)
        if((status != 500) & (status != 200) & (status != 333)):
            print(status)
            print("check errors in "+filename)
            write_or_append_file(filename)
    # You might want to use https://github.com/g2p/rfc6266
    content_disposition = response.headers['content-disposition']
    filename = content_disposition[len('attachment; filename="'):-len('"')]
    return filename, response


# Geocoder votre fichier en une fois s'il est petit.
def geoc(filename,nb):
    geocode("../data-prod/5-geocodage/2-mini-split/"+str(nb)+"/",filename,"../data-prod/5-geocodage/3-mini-split-geocoded/"+str(nb)+"/")
    #geocode("../data-prod/5-geocodage/mini-split/1/",filename,"../data-prod/5-geocodage/geocoded/1/")

# => data.geocoded.csv

import multiprocessing

def main():
    total_begin_time = datetime.datetime.now()

    global listpbfile
    listpbfile = []
    for i in range(97):
        begin_time = datetime.datetime.now()
        nb = i +1
        path = '../data-prod/5-geocodage/2-mini-split/'+str(nb)+'/'
        files = [f for f in os.listdir(path) if f.endswith('.csv')]

        p = multiprocessing.Pool(processes = int(sys.argv[1]))

        for file in files:
            p.apply_async(geoc, [file,nb])

        p.close()
        p.join()

        print("Pour le dossier "+str(nb)+" : ")
        print(datetime.datetime.now()-begin_time)

    print("Total runtime  : ")
    print(datetime.datetime.now()-total_begin_time)


if __name__ == "__main__":
    main()
