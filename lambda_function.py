import json
import os
import boto3
import csv
import pandas as pd
import re
import pykakasi
import requests
import logging
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def check_alnum(word):  # alphanumeric
    alnum = re.compile(r'^[a-zA-Z0-9_-]+$')
    result = alnum.match(word) is not None
    return result

def get_translate_word(word):   # Japanese -> Romaji
    kakasi = pykakasi.kakasi()
    kakasi.setMode("H", "a")        # Hiragana to ascii
    kakasi.setMode("K", "a")        # Katakana to ascii
    kakasi.setMode("J", "a")        # Japanese to ascii
    kakasi.setMode("r", "Hepburn")  # use Hepburn Roman table
    kakasi.setMode("s", True)       # add space
    kakasi.setMode("C", False)      # no capitalize
    conv = kakasi.getConverter()
    result = conv.do(word)
    return result    

def post_slack(message):
    post_url = os.environ['WEBHOOK_URL']
    channel = os.environ['SLACK_CHANNEL']
    requests.post(post_url, data = json.dumps({
        'channel': channel,
        'username': 'autoload',
        'text': message
    }))

def create_master_table_view(view_name, columns, file_path_replace):
    view_str = f'CREATE VIEW {view_name} AS\n'
    view_str += 'SELECT\n'

    for column in columns:
        if column is not None:
            view_str += f'  VALUE:\"{column}\"::string AS \"{column}\",\n'

    view_str = view_str.rsplit(',', 1)[0] + '\n'
    view_str += 'FROM autoload_table\n'
    view_str += f'INNER JOIN (SELECT DISTINCT(FILENAME) as FILENAME FROM autoload_table WHERE PARTPATH=\'{file_path_replace}\' ORDER BY FILENAME DESC LIMIT 1) as LAST_FILENAME\n'
    view_str += f'ON autoload_table.FILENAME = LAST_FILENAME.FILENAME\n'
    view_str += f'WHERE PARTPATH = \'{file_path_replace}\';\n'
    return view_str


def create_history_table_view(view_name, columns, file_path_replace):
    view_str = f'CREATE VIEW {view_name}_HISTORY AS\n'
    view_str += 'SELECT\n'
    for column in columns:
        if column is not None:
            view_str += f'  VALUE:\"{column}\"::string AS \"{column}\",\n'
    view_str = view_str.rsplit(',', 1)[0] + '\n'
    view_str += 'FROM autoload_table\n'
    view_str += f'WHERE PARTPATH = \'{file_path_replace}\';\n'
    return view_str

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        file_key = record['s3']['object']['key']
        file_path = os.path.split(file_key)[0]
        file_name = os.path.split(file_key)[1]
        logger.info(f'[file_key]{file_key}')
        s3 = boto3.client('s3')
        try:
            csvfile = s3.get_object(Bucket=bucket, Key=file_key)
        except Exception as e:
            post_slack('S3 GetObject:' + file_key + ', Exception:' + e.args[0])
            sys.exit()
        # CSV File Size Check.
        file_size = csvfile['ContentLength']
        if file_size >= 100000000: # 100MByte Over
            post_slack('S3 Object:' + file_key + ' is 100MB Over. size:{:.1f}MB.'.format(file_size / 1000000))
        try:
            csvcontent = csvfile['Body'].read().decode('utf8').split('\n')
            logger.info('file format:utf8')
        except UnicodeDecodeError:
            csvfile = s3.get_object(Bucket=bucket, Key=file_key)
            try:
                csvcontent = csvfile['Body'].read().decode('sjis').split('\n')
                logger.info('file format:sjis')
            except UnicodeDecodeError:
                csvfile = s3.get_object(Bucket=bucket, Key=file_key)
                try:
                    csvcontent = csvfile['Body'].read().decode('cp932').split('\n')
                    logger.info('file format:cp932')
                except Exception as e:
                    post_slack('S3 GetObject:' + file_key + ', Exception:' + e.args[0])
            except Exception as e:
                post_slack('S3 GetObject:' + file_key + ', Exception:' + e.args[0])
        except Exception as e:
            post_slack('S3 GetObject:' + file_key + ', Exception:' + e.args[0])
        data = []
        try:
            csv_file = csv.DictReader(csvcontent)
            data = list(csv_file)
            df = pd.DataFrame(data)
        except Exception as e:
            post_slack('csv read error:' + file_key + ', Exception:' + e.args[0])
        columns = df.columns.values
        for column in columns:
            if column is not None:
                try:
                    if not check_alnum(column): # 列名が日本語
                        result = get_translate_word(column) # ローマ字に変換
                        df = df.rename(columns={column:result}) # 列名変更
                except Exception as e:
                    post_slack('Column Check. S3 GetObject:' + file_key + ', Exception:' + e.args[0])
        view_name = file_name.split('_', 1)[0]
        file_path_replace = file_path.replace('/','')
        columns = df.columns.values # 列名のローマ字変換を反映
        # master table viewとhistory table viewを生成
        master_table_view = create_master_table_view(view_name, columns, file_path_replace)
        history_table_view = create_history_table_view(view_name, columns, file_path_replace)
        # 結合
        view_str = master_table_view
        view_str += '\n' + history_table_view
        os.chdir('/tmp')
        JSON_PATH = file_key[:-4] + ".json"
        try:
            df.to_json('/tmp/tmpfile', orient='records', force_ascii=False, lines=True)
        except Exception as e:
            post_slack('CSV to Json Convert Exception:' + e.args[0])
        try:
            s3.upload_file('/tmp/tmpfile', bucket, JSON_PATH)
        except Exception as e:
            post_slack('S3 Upload Error:' + JSON_PATH + ', Exception:' + e.args[0])
        try:
            TABLE_PATH = file_key[:-4] + ".table"
            file = open('/tmp/tmptablefile', 'w')
            file.write(view_str)
            file.close()
            s3.upload_file('/tmp/tmptablefile', bucket, TABLE_PATH)
        except Exception as e:
            post_slack('S3 Upload Error:' + TABLE_PATH + ', Exception:' + e.args[0])
