#!/usr/bin/env python

import os
import re
import sys
import argparse
import logging

from boto3 import resource, client
from botocore.exceptions import ClientError
import pandas


def get_list_of_s3_buckets():
    result = dict()
    s3 = resource('s3')
    s3_client = client('s3')
    logging.info('Listing all buckets...')
    # pylint: disable=E1101
    buckets = s3.buckets.all()
    for bucket in buckets:
        try:
            logging.info('Checking tags for bucket {}'.format(bucket.name))
            response = s3_client.get_bucket_tagging(Bucket=bucket.name)
            result[bucket.name] = response['TagSet']
        except ClientError:
            logging.debug(bucket.name, "does not have tags, adding tags")
            result[bucket.name] = list()
    return result


def buckets_list_to_dataframe(buckets_list):
    data = list()
    columns = list()
    for bucket_name in buckets_list.keys():
        bucket = dict()
        bucket['BucketName'] = bucket_name
        for tag in buckets_list[bucket_name]:
            bucket[tag['Key']] = tag['Value']
        data.append(bucket)
        columns = list(set().union(columns, bucket.keys()))
    df = pandas.DataFrame(data, columns=columns)
    # Sort columns by name
    df = df.reindex(sorted(df.columns), axis=1)
    return df


def parse_args(args):
    parser = argparse.ArgumentParser(description='Discover buckets and tags that they have')
    parser.add_argument('--debug', action='store_true', default=False,
                        help="enable debug printouts")
    return parser.parse_args()


def main():
    args = parse_args(sys.argv[1:])

    logging_level = logging.INFO if not args.debug else logging.DEBUG
    logging.basicConfig(format='%(levelname)s:  %(message)s', level=logging_level)

    logging.info('Collecting S3 tags data...')

    buckets = get_list_of_s3_buckets()

    logging.info('Collected tags from {} buckets'.format(len(buckets)))

    logging.info('Parsing tags data...')

    df = buckets_list_to_dataframe(buckets)

    report_filename = './s3-buckets.xls'
    logging.info('Writing report to {}...'.format(report_filename))
    df.to_excel(report_filename, index=False)

    logging.info('Done')


if __name__ == '__main__':
    main()