import requests
from watson_developer_cloud.natural_language_understanding_v1 \
    import Features, EntitiesOptions, KeywordsOptions, ConceptsOptions, MetadataOptions
from watson_developer_cloud import NaturalLanguageUnderstandingV1
import boto3
import json
import psycopg2
import configparser
import os
from urllib.parse import urlparse

if __name__ == '__main__':
    requests.packages.urllib3.disable_warnings()
    # Connect to Postgres server.
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    conn = psycopg2.connect(host=config['database']['host'],
                            dbname=config['database']['dbname'],
                            user=config['database']['user'],
                            password=config['database']['password'])
    cur = conn.cursor()

    # Retrieve AWS credentials and connect to Simple Queue Service (SQS).
    cur.execute("""select * from aws_credentials;""")
    aws_credential = cur.fetchone()
    aws_session = boto3.Session(
        aws_access_key_id=aws_credential[0],
        aws_secret_access_key=aws_credential[1],
        region_name=aws_credential[2]
    )
    sqs = aws_session.resource('sqs')
    credentials_queue = sqs.get_queue_by_name(QueueName='watson-credentials')

    exhausted_credentials = False
    received_credentials = credentials_queue.receive_messages()
    if len(received_credentials) == 0:
        exhausted_credentials = True
    else:
        credentials = json.loads(received_credentials[0].body)
        received_credentials[0].delete()
        natural_language_understanding = NaturalLanguageUnderstandingV1(
            username=credentials['username'],
            password=credentials['password'],
            version='2018-03-16')

    queue_empty = False
    queue = sqs.get_queue_by_name(QueueName='watson-urls')

    while not queue_empty and not exhausted_credentials:
        message = queue.receive_messages()
        if len(message) == 0:
            queue_empty = True
        else:
            received_message = json.loads(message[0].body)
            message[0].delete()
            urls = received_message['urls']
            while urls != []:
                url = urls.pop(0)
                insert_record = True
                final_url = None
                status_code = 0
                error = None
                headers = None
                parsed_url = None
                watson = None
                try:
                    r = requests.head(url, allow_redirects=True, verify=False, timeout=10)
                    final_url = r.url
                    status_code = r.status_code
                    headers = r.headers

                    if status_code == 403 or \
                            (status_code == 200 and ('text/html' in r.headers['Content-Type'] or 'text/plain' in r.headers['Content-Type'])):
                        try:
                            watson = natural_language_understanding.analyze(
                                url=url,
                                language=received_message['language'],
                                features=Features(entities=EntitiesOptions(),
                                                  concepts=ConceptsOptions(),
                                                  keywords=KeywordsOptions(),
                                                  metadata=MetadataOptions()),
                                return_analyzed_text=True
                            )
                        except Exception as e:
                            error = repr(e)
                            if e.__class__.__name__ == 'WatsonApiException':
                                if e.code == 429:
                                    insert_record = False
                                    urls.append(url)
                                    received_credentials = credentials_queue.receive_messages()
                                    if len(received_credentials) == 0:
                                        exhausted_credentials = True
                                        message_body = {
                                            'query_alias': received_message['query_alias'],
                                            'urls': urls
                                        }
                                        queue.send_message(MessageBody=json.dumps(message_body))
                                    else:
                                        credentials = json.loads(received_credentials[0].body)
                                        received_credentials[0].delete()
                                        natural_language_understanding = NaturalLanguageUnderstandingV1(
                                            username=credentials['username'],
                                            password=credentials['password'],
                                            version='2018-03-16')
                except Exception as e:
                    error = repr(e)

                if insert_record:
                    if status_code < 400 and final_url is not None:
                        parsed_url = urlparse(final_url)
                    else:
                        parsed_url = urlparse(url)
                    parsed_url = json.dumps(dict(parsed_url._asdict()))

                    if watson is not None:
                        watson = json.dumps(watson)
                        watson = watson.replace('\\u0000', ' ')
                    if headers is not None:
                        headers = json.dumps(dict(headers))

                    cur.execute("""insert into watson
                                    (query_alias, url, response, headers,
                                    status_code, final_url, parsed_url)
                                    values 
                                    (%s, %s, %s, %s, %s, %s, %s)""",
                                (received_message['query_alias'],
                                 url,
                                 watson,
                                 headers,
                                 status_code,
                                 final_url,
                                 parsed_url))
                    conn.commit()

    conn.close()
