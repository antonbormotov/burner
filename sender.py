#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import ConfigParser
import logging
import datetime
from Sender import Sender


if __name__ == "__main__":

    #  Configure logging
    app_logger = logging.getLogger(__name__)
    app_logger.setLevel(logging.INFO)

    handler = logging.FileHandler(
        filename='sender_%s.log' % datetime.datetime.now().strftime('%Y%m%d'),
        mode='a'
    )
    handler.setLevel(logging.INFO)
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M"))
    app_logger.addHandler(handler)

    app_logger.info('Started')

    #  Read configuration file
    config = ConfigParser.RawConfigParser()
    config.read('config.cfg')

    sender = Sender.Sender(config, app_logger)

    app_logger.info('Completed\n')
