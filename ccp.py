#!/usr/bin/env python
import os
import argparse
import uuid
import sys
import logging

import boto
import urlparse
import yaml


logging.basicConfig(level=logging.INFO)

# URI scheme for Google Cloud Storage.
GOOGLE_STORAGE = 'gs'
# URI scheme for accessing local files.
LOCAL_FILE = 'file'

def save_to(lst, fname):
    with open(fname, 'wb') as f:
        for l in lst:
            f.write(l)
            f.write('\n')

class CopyMachine(object):

    def __init__(self, config, srcfiles, dstFolder, tmpFolder='/tmp/'):
        self.buckets = {}
        self.gs_config = config['gs']
        self.gs_conn = boto.connect_gs(
            self.gs_config['api_key'], self.gs_config['api_secret'])
        

        self.s3_config = config['s3']
        self.s3_conn = boto.connect_s3(
            self.s3_config['api_key'], self.s3_config['api_secret'])

        self.srcfiles = srcfiles
        self.dstFolder = dstFolder

        self.success_list = []
        self.failed_list = []
        self.tmpFolder = tmpFolder
        self.logger = logging.getLogger(CopyMachine.__name__)

    def get_bucket(self, scheme, bucket_name):
        key = '{}://{}'.format(scheme, bucket_name)
        if key not in self.buckets:
            if scheme == 'gs':
                bucket = self.gs_conn.get_bucket(bucket_name)

            elif scheme == 's3':
                bucket = self.s3_conn.get_bucket(bucket_name)
            else:
                raise Exception('Unspported scheme: ' + scheme)

            self.buckets[key] = bucket

        return self.buckets[key]

    def execute_one(self, url):

        last_e = None
        fpath = None
        uparse_url = urlparse.urlparse(url)
        uparse_dstFolder = urlparse.urlparse(self.dstFolder)

        try:
            for i in xrange(3):
                try:
                    fpath = self.download(uparse_url)
                    last_e = None
                    break
                except Exception, e:
                    last_e = e

            if last_e:
                raise last_e

            for i in xrange(3):
                try:
                    self.upload(fpath, uparse_url.path, uparse_dstFolder)
                    last_e = None
                    break
                except Exception, e:
                    last_e = e
                    break

            if last_e:
                raise last_e

        finally:
            if fpath:
                os.remove(fpath)

    def copy_all(self):
        for url in self.srcfiles:
            try:
                self.execute_one(url)
                self.success_list.append(url)
                logging.info('%s ok' % url)
            except:
                self.failed_list.append(url)
                logging.warn('%s failed' % url)

            self.save_list()

    def download(self, url_result):
        self.logger.info('<<downloading %s' % url_result.path)
        bucket = self.get_bucket(
            url_result.scheme,
            url_result.netloc)

        obj = bucket.get_key(url_result.path)
        fpath = os.path.join(self.tmpFolder, str(uuid.uuid4()))
        obj.get_contents_to_filename(fpath)
        return fpath

    def upload(self, fpath, uri_name, uparse_dstFolder):
        if uri_name.startswith('/'):
            uri_name = uri_name[1:]

        bucket = self.get_bucket(uparse_dstFolder.scheme, uparse_dstFolder.netloc)
        key = os.path.join(uparse_dstFolder.path, uri_name)
        self.logger.info('>>uploading %s' % key)
        obj = bucket.new_key(key)
        obj.set_contents_from_filename(fpath)

    def save_list(self):
        if self.success_list:
            save_to(self.success_list, 'success.txt')
        if self.failed_list:
            save_to(self.failed_list, 'failed.txt')


parser = argparse.ArgumentParser(description='gs2s3. A Google storage to S3 Synchronization tool.')
parser.add_argument('-s', '--src', type=str, nargs='+',
    help='src',
    default=[])

parser.add_argument('-S', '--srcFile', type=str,
    help='Src files',
    default=None)


parser.add_argument('-d', '--dstFolder', type=str, 
    help='Destination folder', default=None)

parser.add_argument('-c', '--config', type=str, 
    help='Destination folder', default='config.yaml')

parser.add_argument('--tmpFolder', type=str, 
    help='Destination folder', default='/tmp')

def real_path(path):
    ret_path = path
    if not path.startswith('/'):
        ret_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), 
                path
            )
        )
    return ret_path

def main(args):
    config_path = real_path(args.config)
    config = {}

    with open(config_path, 'rb') as f:
        config =  yaml.load(f.read())
    import ipdb; ipdb.set_trace()
    if args.srcFile:
        with open(args.srcFile, 'rb') as f:
            args.src = [ l[:-1] for l in f.readlines()]


    if not args.src or not args.dstFolder:
        print "Please provide at least src and dst"
        sys.exit(1)

    copy_machine = CopyMachine(config, 
        args.src, args.dstFolder, args.tmpFolder)

    copy_machine.copy_all()

    return 0

if __name__ == "__main__":
    args = parser.parse_args()
    main(args)


