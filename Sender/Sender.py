#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import smtplib
import datetime
import elasticsearch
from tabulate import tabulate
from email.header import Header
from email.utils import formataddr
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from elasticsearch import Elasticsearch, RequestsHttpConnection


class Sender:
    config = None
    smtp_server = None
    es_client = None
    MAX_DOCS = 100
    emails = []

    def __init__(self, config, logger):
        self.logger = logger
        self.config = config
        params = {
            'hosts': "https://{}:{}@{}".format(
                config.get('elasticsearch', 'ES_USERNAME'),
                config.get('elasticsearch', 'ES_PASSWORD'),
                config.get('elasticsearch', 'ES_HOST')
            ),
            'connection_class': RequestsHttpConnection,
            'use_ssl': True,
            'verify_certs': True
        }
        self.es_client = Elasticsearch(**params)
        self.smtp_server = smtplib.SMTP(config.get('email', 'SMTP'), 587)
        self.send_message()

    def send_message(self):
        msg = MIMEMultipart(
            "alternative",
            None,
            [
                MIMEText(self.build_report()['text']),
                MIMEText(self.build_report()['html'], 'html')
            ]
        )
        msg['From'] = formataddr((str(Header('iPriceBot - Burner', 'utf-8')), self.config.get('email', 'MAILBOX')))
        msg['To'] = self.config.get('email', 'TO')
        msg['Subject'] = self.build_subject()

        self.smtp_server.starttls()
        self.smtp_server.login(
            self.config.get('email', 'MAILBOX'),
            self.config.get('email', 'PASSWORD')
        )
        self.smtp_server.sendmail(self.config.get('email', 'MAILBOX'), self.config.get('email', 'TO'), msg.as_string())
        self.smtp_server.quit()

    def build_subject(self):
        return 'Weekly report {}'.format(datetime.datetime.now().isocalendar()[1])

    def build_report(self):
        res = {}
        text = html = """
        Table below displays weekly expenses on applications deployed in qa environment using Teamcity CI.
        <br/>Expenses include EC2 and EBS charges.<br/>
        <br/>{table}<br/>
        """
        try:
            res = self.es_client.search(
                index='burner_w{}'.format(datetime.datetime.now().isocalendar()[1]),
                doc_type='user',
                body={
                    "_source": [
                        "user"
                    ],
                    "size": self.MAX_DOCS,
                    "sort": {
                        "_script": {
                            "type": "number",
                            "script": "doc['total_ec2_spent'].value + doc['total_ebs_spent'].value",
                            "order": "desc"
                        }
                    },
                    "query": {
                        "match_all": {}
                    }
                },
                request_timeout=300
            )
        except elasticsearch.exceptions.ElasticsearchException as e:
            self.logger.info('Elasticsearch exception: \n {}'.format(e))

        data = list()
        data.append(
            [
                "User",
                "Expenses, USD"
            ]
        )
        i = 0
        s = 0
        for hit in res['hits']['hits']:
            self.emails.append(hit['_source']['user'])
            i = i + 1
            s = s + round(hit['sort'][0], 2)
            data.append(
                [
                    "{:02d}. {:>50}".format(i, hit['_source']['user']),
                    "Spent {:05.2f}".format(round(hit['sort'][0], 2))
                ]
            )
        data.append(
            [
                "Total:",
                "<strong>${:05.2f}</strong>".format(s)
            ]
        )
        text = text.format(
            table=tabulate(data, headers="firstrow", tablefmt="grid")
        )
        html = html.format(
            table=tabulate(data, headers="firstrow", tablefmt="html")
        )
        return {
            'text': text,
            'html': html
        }
