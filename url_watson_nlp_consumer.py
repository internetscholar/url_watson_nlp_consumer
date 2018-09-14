import requests
from watson_developer_cloud.natural_language_understanding_v1 \
    import Features, EntitiesOptions, KeywordsOptions, ConceptsOptions, \
    MetadataOptions, EmotionOptions, SentimentOptions
from watson_developer_cloud import NaturalLanguageUnderstandingV1
import boto3
import json
import psycopg2
import configparser
import os
import logging
from psycopg2 import extras
import urllib3
import traceback


def main():
    if 'DISPLAY' not in os.environ:
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')
    else:
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    ip = requests.get('http://checkip.amazonaws.com').text.rstrip()
    # Connect to Postgres server.
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    conn = psycopg2.connect(host=config['database']['host'],
                            dbname=config['database']['db_name'],
                            user=config['database']['user'],
                            password=config['database']['password'])
    cur = conn.cursor(cursor_factory=extras.RealDictCursor)
    logging.info('Connected to database')

    # Retrieve AWS credentials and connect to Simple Queue Service (SQS).
    cur.execute("""select * from aws_credentials;""")
    aws_credential = cur.fetchone()
    aws_session = boto3.Session(
        aws_access_key_id=aws_credential['aws_access_key_id'],
        aws_secret_access_key=aws_credential['aws_secret_access_key'],
        region_name=config['aws']['region_queues']
    )
    sqs = aws_session.resource('sqs')
    logging.info('Connected to AWS')
    credentials_queue = sqs.get_queue_by_name(QueueName='watson_credentials')

    exhausted_credentials = False
    received_credentials = credentials_queue.receive_messages()
    if len(received_credentials) != 0:
        credentials = json.loads(received_credentials[0].body)
        received_credentials[0].delete()
        logging.info('Acquired credential')
        natural_language_understanding = NaturalLanguageUnderstandingV1(
            username=credentials['username'],
            password=credentials['password'],
            version='2018-03-16')
        logging.info('Connected to Watson')
        queue_empty = False
        record = None
        queue = sqs.get_queue_by_name(QueueName='url_watson_nlp')
        try:
            while not queue_empty and not exhausted_credentials:
                message = queue.receive_messages()
                if len(message) == 0:
                    queue_empty = True
                else:
                    received_message = json.loads(message[0].body)
                    message[0].delete()
                    logging.info('Going to process %d URLs', len(received_message))
                    for record in received_message:
                        logging.info('Going to process URL %s - %s', record['project_name'], record['url'])
                        error = None
                        try:
                            watson = natural_language_understanding.analyze(
                                url=record['url'],
                                features=Features(entities=EntitiesOptions(sentiment=True, emotion=True),
                                                  concepts=ConceptsOptions(),
                                                  keywords=KeywordsOptions(sentiment=True, emotion=True),
                                                  metadata=MetadataOptions(),
                                                  emotion=EmotionOptions(),
                                                  sentiment=SentimentOptions()),
                                return_analyzed_text=True
                            )
                        except Exception as e:
                            logging.info('Watch out! There is an error: %s', str(e))
                            error = traceback.format_exc()
                        logging.info('Success!')
                        watson = json.dumps(watson)
                        watson = watson.replace('\\u0000', ' ')

                        cur.execute("""insert into url_watson_nlp
                                        (url, project_name, response, error)
                                        values 
                                        (%s, %s, %s, %s)""",
                                    (record['url'],
                                     record['project_name'],
                                     watson,
                                     error,))
                        conn.commit()
                        logging.info('Saved on the database')
        except Exception:
            conn.rollback()
            # add record indicating error.
            cur.execute("insert into error (current_record, error, module, ip) VALUES (%s, %s, %s, %s)",
                        (json.dumps(record),
                         traceback.format_exc(),
                         'url_watson_nlp_consumer',
                         ip), )
            logging.info('Saved record with error information')
            conn.commit()
            raise
    conn.close()
    logging.info('Database closed')


if __name__ == '__main__':
    main()
