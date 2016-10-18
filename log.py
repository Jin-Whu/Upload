#!/usr/bin/env python
# coding:utf-8

import logging
import datetime
from enum import Enum, unique
logging.basicConfig(level=logging.DEBUG)


@unique
class Level(Enum):
    """Level class."""
    DEBUG = 1
    INFO = 2
    WARNING = 3
    ERROR = 4
    CRITICAL = 5


class Message(object):
    """Message class."""

    def __init__(self, message, level):
        self.message = ' '.join([str(datetime.datetime.now()), message])
        self.level = level

    def log(self):
        """Log."""
        if self.level == Level.DEBUG:
            logging.debug(self.message)
        elif self.level == Level.INFO:
            logging.info(self.message)
        elif self.level == Level.WARNING:
            print 1
            logging.warning(self.message)
        elif self.level == Level.ERROR:
            logging.error(self.message)
        elif self.level == Level.CRITICAL:
            logging.critical(self.message)
