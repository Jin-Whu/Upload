#!/usr/bin/env python
# coding:utf-8

import argparse
import ftplib
import log
import os
import time
import re
from collections import namedtuple


class Config(object):
    """configure value.

    Attributes:
        interlval:interval to upload, in seconds, type:int.
        path:file path to upload, type:list.
        ftp:ftp, type:namedtuple.
        filter:file filter.
    """

    def __init__(self, interval, path, prefix, suffix, rule, regex, ftp):
        self.interval = interval
        self.path = path
        self.prefix = prefix
        self.suffix = suffix
        self.rule = rule
        self.regex = regex
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

        if not self.regex:
            if self.rule not in ['sgl', 'com']:
                message = log.Message('rule must be sgl or com', log.Level.ERROR)
                message.log()
                return 0

        if self.ftp.keep not in ['yes', 'no']:
            message = log.Message('ftp keep must be yes or no', log.Level.ERROR)
            Message.log()
            return 0

        filefilter = namedtuple('Filter', ('prefix', 'suffix', 'rule', 'regex'))
        self.filter = filefilter(self.prefix, self.suffix, self.rule, self.regex)

        return 1


def upload(config):
    """Upload file."""
    while True:
        session = ftplib.FTP(host=config.ftp.host,
                             user=config.ftp.user,
                             passwd=config.ftp.password)
        message = log.Message('Login to %s' % config.ftp.host, log.Level.INFO)
        message.log()
        # file filter
        if config.ftp.dir:
            try:
                session.cwd(config.ftp.dir)
            except:
                pass
        filefilter = config.filter
        for path in config.path:
            try:
                ftpupload(session, path, config.ftp.keep, filefilter)
            except:
                pass
        try:
            session.quit()
        except:
            pass
        message = log.Message('Upload done!', log.Level.INFO)
        message.log()
        if not config.interval:
            break
        time.sleep(config.interval)


def ftpupload(session, path, keep, filefilter):
    """Upload file.

    Args:
        session:ftp session.
        path:upload path.
        keep:keep directory structure.
        filefilter:file filter.
    """
    uploadflag = True
    if keep == 'yes':
        directory = os.path.split(path)[1]
        if directory:
            try:
                session.mkd(directory)
            except:
                pass
            try:
                session.cwd(directory)
            except:
                pass
    for subpath in os.listdir(path):
        localpath = os.path.join(path, subpath)
        if os.path.isfile(localpath):
            if subpath in session.nlst():
                if os.stat(localpath).st_size <= session.size(subpath):
                    continue
            if not filefilter.regex:
                if filefilter.rule == 'sgl':
                    if filefilter.prefix:
                        for prefix in filefilter.prefix:
                            if not subpath.startswith(prefix):
                                uploadflag = False
                            else:
                                uploadflag = True
                                break
                    if not uploadflag and filefilter.suffix:
                        for suffix in filefilter.suffix:
                            if not subpath.endswith(suffix):
                                uploadflag = False
                            else:
                                uploadflag = True
                                break
                elif filefilter.rule == 'com':
                    for prefix, suffix in zip(filefilter.prefix, filefilter.suffix):
                        if subpath.startswith(prefix) and subpath.endswith(suffix):
                            uploadflag = True
                            break
                        else:
                            uploadflag = False
            else:
                for regex in filefilter.regex:
                    if re.match(r'%s' % regex, subpath):
                        uploadflag = True
                        break
                    else:
                        uploadflag = False
            if not uploadflag:
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
            ftpupload(session, localpath, keep, filefilter)
            if keep == 'yes':
                session.cwd('..')


def configure(config_path):
    """read configure file."""
    if not os.path.exists(config_path):
        message = log.Message('Not find %s' % config_path, log.Level.ERROR)
        message.log()
        return 0

    interval = None
    path = list()
    prefix = list()
    suffix = list()
    rule = None
    regex = list()
    ftp = namedtuple('FTP', ('host', 'user', 'password', 'dir', 'keep'))
    host = None
    user = None
    password = None
    directory = None
    keep = None
    with open(config_path) as f:
        for line in f:
            if line.startswith('interval'):
                interval = line.split('=')[1].strip()

            if line.startswith('path'):
                path.extend(line.split('=')[1].split())

            if line.startswith('host'):
                host = line.split('=')[1].strip()

            if line.startswith('user'):
                user = line.split('=')[1].strip()

            if line.startswith('password'):
                password = line.split('=')[1].strip()

            if line.startswith('dir'):
                directory = line.split('=')[1].strip()

            if line.startswith('keep'):
                keep = line.split('=')[1].strip()

            if line.startswith('prefix'):
                prefix.extend(line.split('=')[1].split())

            if line.startswith('suffix'):
                suffix.extend(line.split('=')[1].split())

            if line.startswith('rule'):
                rule = line.split('=')[1].strip()

            if line.startswith('regex'):
                regex.extend(line.split('=')[1].split())


    config = Config(interval, path, prefix, suffix, rule, regex, ftp(host, user, password, directory, keep))
    if not config.check():
        return 0
    return config


def main(configpath):
    """script main entry."""
    config = configure(configpath)  # read configure file
    if not config:
        return 0

    # start upload
    upload(config)


if __name__ == '__main__':
    parser = argparse.ArgumentParser('uploader')
    parser.add_argument('path', type=str, help='configure path')
    args = parser.parse_args()
    main(args.path)
