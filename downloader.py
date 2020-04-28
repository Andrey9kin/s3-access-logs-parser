#!/usr/bin/env python

import os
import re
import sys
import argparse
import logging

from boto3 import resource
import botocore


def create_if_does_not_exist(input_path):
    if not os.path.exists(input_path):
        logging.info('Path {} does not exist. Creating...'.format(input_path))
        os.makedirs(input_path)


def get_list_of_objects_to_download(s3_bucket, prefix):
    objects = s3_bucket.objects.filter(Prefix=prefix)
    objects_list = list(objects)
    if len(objects_list) == 0:
        logging.error('Can not find any objects. Please check prefix and try again')
        sys.exit(1)
    return objects_list


def download_object(s3_bucket, dest, obj):
    _, filename = os.path.split(obj.key)
    dest_path = os.path.join(dest, filename)
    if os.path.exists(dest_path):
        logging.debug('{} already exists, skipping download'.format(dest_path))
        return
    logging.debug('Downloading {} to {}'.format(obj.key, dest_path))
    s3_bucket.download_file(obj.key, dest_path)


def download(bucket, prefix, dest):
    try:
        s3_resource = resource('s3')
        # pylint: disable=E1101
        s3_bucket = s3_resource.Bucket(bucket)
        objects_list = get_list_of_objects_to_download(s3_bucket, prefix)
        logging.info('Downloading {} objects...'.format(len(objects_list)))
        count = 0
        for obj in objects_list:
            download_object(s3_bucket, dest, obj)
            count += 1
            if count%500 == 0:
                logging.info('Downloaded {} objects'.format(count))
    except botocore.exceptions.NoCredentialsError as no_credentials_error:
        logging.error('Failed to download. It looks like you need to configure your AWS creds')
        logging.error('Error was: {}'.format(no_credentials_error))
        sys.exit(1)
    except botocore.exceptions.ClientError as client_error:
        logging.error('Failed to download. Do you have access or did you spell bucket name right?')
        logging.error('Error was: {}'.format(client_error))
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(1)


def string_to_safe_filename(filename):
    safe_filename = re.sub(r'[^\w\d-]', '_', filename)
    logging.debug('Converted string {} to safe file name {}'.format(filename, safe_filename))
    return safe_filename


def parse_args(args):
    parser = argparse.ArgumentParser(description='Download S3 access logs')
    parser.add_argument('bucket',
                        help='bucket name to download from')
    parser.add_argument('prefix',
                        help='prefix to filter out objects to download')
    parser.add_argument('logs_dest',
                        help="path to where access files should be placed to. will be created if does not exists")
    parser.add_argument('--debug', action='store_true', default=False,
                        help="enable debug printouts")
    return parser.parse_args()


def main():
    args = parse_args(sys.argv[1:])

    logging_level = logging.INFO if not args.debug else logging.DEBUG
    logging.basicConfig(format='%(levelname)s:  %(message)s', level=logging_level)

    logs_dest = os.path.join(args.logs_dest, args.bucket, string_to_safe_filename(args.prefix))
    create_if_does_not_exist(logs_dest)

    logging.info('Downloading objects with prefix {} from bucket {}...'.format(args.prefix, args.bucket))

    download(args.bucket, args.prefix, logs_dest)

    logging.info('Done')


if __name__ == '__main__':
    main()
