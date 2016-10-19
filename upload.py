#!/usr/bin/env python
# coding:utf-8

import ftplib
import log
import os
import time
import datetime
import multiprocessing
from collections import namedtuple


class Config(object):
    """configure value.

    Attributes:
        interlval:interval to upload, in seconds, type:int.
        path:file path to upload, type:list.
        ftp:ftp, type:namedtuple.
    """

    def __init__(self, interval, path, ftp):
        self.interval = interval
        self.path = path
        self.ftp = ftp

    def check(self):
        """check interval and path if valid."""
        if not self.interval or not self.path:
            message = log.Message('Read configure file failed!',
                                  log.Level.ERROR)
            message.log()
            return 0

        if not self.interval.isdigit():
            message = log.Message('interval is not interger', log.Level.ERROR)
            message.log()
            return 0
        self.interval = int(self.interval)

        for filepath in self.path:
            if not os.path.exists(filepath):
                message = log.Message('Not find path to upload: %s' % filepath,
                                      log.Level.ERROR)
                message.log()
                return 0

        if not self.ftp.host or not self.ftp.user or not self.ftp.password:
            message = log.Message('Missing ftp information', log.Level.ERROR)
            message.log()
            return 0
        return 1


def upload(config):
    """Upload file."""
    while True:
        session = ftplib.FTP(host=config.ftp.host,
                             user=config.ftp.user,
                             passwd=config.ftp.password)
        message = log.Message('Login to %s' % config.ftp.host, log.Level.INFO)
        message.log()
        upload_date = (datetime.datetime.now() - datetime.timedelta(
            seconds=config.interval)).date()
        message = log.Message('Strat upload %s\'s file' % str(upload_date),
                              log.Level.INFO)
        message.log()
        for path in config.path:
            process = multiprocessing.Process(
                target=ftpupload, args=(session, path, upload_date))
            process.start()
            process.join()
        session.quit()
        message = log.Message('Upload done!', log.Level.INFO)
        message.log()
        time.sleep(config.interval)


def ftpupload(session, path, upload_date):
    """Upload file.

    Args:
        session:ftp session.
        path:upload path.
        upload_date:upload file date.
    """
    topdir = os.path.split(path)[1]
    try:
        session.mkd(topdir)
    except:
        pass
    session.cwd(topdir)
    for subpath in os.listdir(path):
        localpath = os.path.join(path, subpath)
        if os.path.isfile(localpath):
            if subpath in session.nlst():
                continue
            timestamp = datetime.datetime.fromtimestamp(
                os.path.getmtime(localpath)).date()
            if timestamp != upload_date:
                continue
            try:
                with open(localpath, 'rb') as f:
                    session.storbinary('STOR %s' % subpath, f, 1024)
                message = log.Message('Upload %s successful!' % localpath,
                                      log.Level.INFO)
                message.log()
            except:
                message = log.Message('Upload %s failed!' % localpath,
                                      log.Level.WARNING)
                message.log()
        elif os.path.isdir(localpath):
            ftpupload(session, localpath, upload_date)
            session.cwd('..')


def configure():
    """read configure file."""
    config_path = os.path.join(os.path.dirname(__file__), 'configure.ini')
    if not os.path.exists(config_path):
        message = log.Message('Not find %s' % config_path, log.Level.ERROR)
        message.log()
        return 0

    interval = None
    path = list()
    ftp = namedtuple('FTP', ('host', 'user', 'password'))
    host = None
    user = None
    password = None
    with open(config_path) as f:
        for line in f:
            if line.startswith('interval'):
                interval = line.split('=')[1].strip()

            if line.startswith('path'):
                path.append(line.split('=')[1].strip())

            if line.startswith('host'):
                host = line.split('=')[1].strip()

            if line.startswith('user'):
                user = line.split('=')[1].strip()

            if line.startswith('password'):
                password = line.split('=')[1].strip()
    config = Config(interval, path, ftp(host, user, password))
    if not config.check():
        return 0
    return config


def main():
    """script main entry."""
    config = configure()  # read configure file
    if not config:
        return 0

    # start upload
    upload(config)


if __name__ == '__main__':
    main()
