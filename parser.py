#!/usr/bin/env python

import os
import re
import sys
import argparse
import logging

import pandas


def create_if_does_not_exist(input_path):
    if not os.path.exists(input_path):
        logging.info('Path {} does not exist. Creating...'.format(input_path))
        os.makedirs(input_path)


def string_to_safe_filename(filename):
    safe_filename = re.sub(r'[^\w\d-]', '_', filename)
    logging.debug('Converted string {} to safe file name {}'.format(filename, safe_filename))
    return safe_filename


def aggregate_log_files_to_dataframe(input_path, output_path):
    df_list = []
    for file in os.listdir(input_path):
        # read all log files into pandas with proper meaningful header
        # https://docs.aws.amazon.com/AmazonS3/latest/dev/LogFormat.html
        df_list.append(pandas.read_csv(
            '{0}/{1}'.format(input_path, file),
            sep=" ",
            names=['BucketOwner', 'Bucket', 'Date', 'TimeOffset', 'RemoteIP', 'RequesterARN/CanonicalID',
               'RequestID',
               'Operation', 'Key', 'Request-URI', 'HTTPstatus', 'ErrorCode', 'BytesSent', 'ObjectSize',
               'TotalTime',
               'Turn-AroundTime', 'Referrer', 'User-Agent', 'VersionId', 'HostId', 'SignatureVersion',
               'CipherSuite',
               'AuthenticationType', 'HostHeader', 'TLSversion'],
        usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24],
        engine='python'))

    df = pandas.concat(df_list)
    df.sort_values(by=['Date'], inplace=True, ascending=True)
    complete_df_filename = os.path.join(output_path, 's3-accesslog-complete.xls')
    logging.info('Writing full data frame into {}'.format(complete_df_filename))
    df.to_excel(complete_df_filename, index=False)

    return df


def arn_sorted_reports(df, output_path):
    logging.info('Writing reports sorted by caller ARN...')
    dfgb = df.groupby('RequesterARN/CanonicalID')
    for name, group in dfgb:
        df_filename = os.path.join(output_path, string_to_safe_filename(name))
        trimmed_df = group.drop(columns=['BucketOwner', 'RequesterARN/CanonicalID', 'RequestID'])
        logging.info('Writing {}'.format(df_filename))
        trimmed_df.to_excel('{}.xls'.format(df_filename), index=False)


def operation_sorted_reports(df, output_path):
    logging.info('Writing reports sorted by operation...')
    dfgb = df.groupby('Operation')
    for name, group in dfgb:
        df_filename = os.path.join(output_path, string_to_safe_filename(name))
        trimmed_df = group.drop(columns=['BucketOwner', 'RequestID'])
        logging.info('Writing {}'.format(df_filename))
        trimmed_df.to_excel('{}.xls'.format(df_filename), index=False)


def parse_args(args):
    parser = argparse.ArgumentParser(description='Analyze S3 access logs')
    parser.add_argument('logs_dest',
                        help="path to where access files should be placed to. will be created if does not exists")
    parser.add_argument('report_dest',
                        help="path to where report files should be placed to. will be created if does not exists")
    parser.add_argument('--debug', action='store_true', default=False,
                        help="enable debug printouts")
    return parser.parse_args()


def operation_and_arn_sorted_reports(df, output_path):
    logging.info('Writing reports sorted by operation and ARN...')
    for operation_name, operation_group in df.groupby('Operation'):
        for arn, arn_group in operation_group.groupby('RequesterARN/CanonicalID'):
            df_filename = os.path.join(output_path, string_to_safe_filename('{}_{}'.format(operation_name, arn)))
            trimmed_df = arn_group.drop(columns=['BucketOwner', 'RequestID'])
            logging.info('Writing {}'.format(df_filename))
            trimmed_df.to_excel('{}.xls'.format(df_filename), index=False)


def main():
    args = parse_args(sys.argv[1:])

    logging_level = logging.INFO if not args.debug else logging.DEBUG
    logging.basicConfig(format='%(levelname)s:  %(message)s', level=logging_level)

    logging.info('Processing access logs from {}'.format(args.logs_dest))

    create_if_does_not_exist(args.report_dest)

    df = aggregate_log_files_to_dataframe(args.logs_dest, args.report_dest)

    logging.info('Generate ARN-sorted reports...')

    arn_sorted_reports(df, args.report_dest)

    logging.info('Generate GET object report...')

    operation_sorted_reports(df, args.report_dest)

    operation_and_arn_sorted_reports(df, args.report_dest)

    logging.info('Done')


if __name__ == '__main__':
    main()
