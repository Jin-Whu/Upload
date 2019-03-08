#!/usr/bin/env python
# coding:utf-8

import argparse
import ftplib
import log
import os
import time
import re
from collections import namedtuple
from datetime import datetime
from datetime import timedelta
import gzip
import shutil
import paramiko
from paramiko import SSHClient
from paramiko import SFTPClient


server = namedtuple('SERVER', ('host', 'user', 'password', 'dir', 'keep'))
filefilter = namedtuple('Filter', ('prefix', 'suffix', 'rule', 'regex', 'pattern'))

GPST0 = datetime(1980, 1, 6, 0, 0, 0)


class Config(object):
    """configure value.

    Attributes:
        interlval: interval to upload, in seconds, type:int.
        path: file path to upload, type:list.
        prefix: prefix, type:list.
        suffix: suffix, type:list;
        rule: combine or single, type:str.
        regex: regex, type:list.
        pattern: pattern, type:list.
        delay: delay, in seconds, type:int.
        server: ftp or ssh server, type:namedtuple.
        mode: upload mode, 0:ftp 1:sftp, type:int.
        zflag: gzip, 0:False 1:True 2:Delete, type:int.
        filter: filter rule, type:namedtuple.
    """

    def __init__(self, interval, path, prefix, suffix, rule, regex, pattern, delay, server, mode, zflag):
        self.interval = interval
        self.path = path
        self.prefix = prefix
        self.suffix = suffix
        self.rule = rule
        self.regex = regex
        self.pattern = pattern
        self.delay = delay
        self.server = server
        self.mode = mode
        self.zflag = zflag
        self.filter = None

    def check(self):
        if not self.path:
            message = log.Message('No path upload!', log.Level.ERROR)
            message.log()
            return 0

        for filepath in self.path:
            if not os.path.isdir(filepath):
                message = log.Message('Not find path to upload: %s.' % filepath,
                                      log.Level.WARNING)
                message.log()
                # return 0 (enable non-existed path)

        if not self.server.host or not self.server.user or not self.server.password:
            message = log.Message('Missing host server information.', log.Level.ERROR)
            message.log()
            return 0

        if not self.regex:
            if self.rule not in ['sgl', 'com']:
                self.rule = 'sgl'

        if self.mode not in [0, 1]:
            self.mode = 0

        if self.zflag not in [0, 1, 2]:
            self.zflag = 0
        
        self.filter = filefilter(
            self.prefix, self.suffix, self.rule, self.regex, self.pattern)

        return 1


class Session(object):
    def __init__(self, server, mode, zflag):
        self.server = server
        self.mode = mode
        self.zflag = zflag
        self.session = None

    def connect(self):
        host = self.server.host.split(':')
        port = 21 if self.mode == 0 else 22
        if len(host) > 1:
            try:
                port = int(host[1])
            except:
                message = log.Message('port: %s is invalid.' %
                                      host[1], log.Level.ERROR)
                message.log()
                return False

        if self.mode == 0:
            self.session = ftplib.FTP()
            self.session.connect(host[0], port)
            self.session.login(self.server.user, self.server.password)
        else:
            ssh = SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(host[0], port, username=self.server.user,
                        password=self.server.password)
            self.session = SFTPClient.from_transport(ssh.get_transport())
        return True

    def close(self):
        try:
            if self.session:
                if self.mode == 0:
                        self.session.quit()
                elif self.mode == 1:
                        self.session.close()
        except:
            pass

    def cwd(self, directory):
        if self.session:
            if self.mode == 0:
                self.session.cwd(directory)
            elif self.mode == 1:
                self.session.chdir(directory)

    def mkd(self, directory):
        if self.session:
            if self.mode == 0:
                self.session.mkd(directory)
            elif self.mode == 1:
                self.session.mkdir(directory)

    def nlst(self):
        if self.session:
            if self.mode == 0:
                return self.session.nlst()
            elif self.mode == 1:
                return self.session.listdir()

    def size(self, filepath):
        if self.session:
            if self.mode == 0:
                return self.session.size(filepath)
            elif self.mode == 1:
                return self.session.stat(filepath).st_size

    def storbinary(self, filepath, blocksize=1024):
        filename = os.path.basename(filepath)
        if self.zflag:
            with open(filepath, 'rb') as f_in:
                with gzip.open(filepath + '.gz', 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            if self.zflag == 2:
                try:
                    os.remove(filepath)
                except:
                    message = log.Message('remove %s failed.' % filepath,
                                          log.Level.ERROR)
                    message.log()
                    os.remove(filepath + '.gz')
                    raise
            filename += '.gz'
            filepath += '.gz'

        if self.session:
            if self.mode == 0:
                with open(filepath, 'rb') as fp:
                    self.session.storbinary('STOR %s' %
                                            filename, fp, blocksize)
            elif self.mode == 1:
                self.session.put(filepath, filename)


class PatternFile(object):
    def __init__(self, pattern, delay):
        self.__pattern = pattern
        self.__t = datetime.utcnow() - timedelta(seconds=delay)
        self.filename = None
        self.__parse()
    

    def __parse(self):
        formats = {
            '{YY}': '%y',
            '{YYYY}': '%Y',
            '{MM}': '%m',
            '{DD}': '%d',
            '{HH}': '%H',
            '{mm}': '%M',
            '{SS}': '%S',
            '{DOY}': '%j'
        }
        week, wday = self.__week(self.__t)
        for pattern in formats:
            if pattern in self.__pattern:
                self.__pattern = self.__pattern.replace(pattern, formats[pattern])
        if '{WEEK}' in self.__pattern:
            self.__pattern = self.__pattern.replace('{WEEK}', str(week))
        if '{WDAY}' in self.__pattern:
            self.__pattern = self.__pattern.replace('{WDAY}', str(wday))
        self.filename = self.__t.strftime(self.__pattern)

    def __week(self, t):
        week = int((t - GPST0).total_seconds() / 7 / 86400)
        wday = (t.timetuple().tm_wday + 1) % 7
        return week, wday



def upload(config):
    """Upload file using ftp."""
    while True:
        session = Session(config.server, config.mode, config.zflag)
        if not session.connect():
            session.close()
            time.sleep(60)
            continue
        message = log.Message('Login to %s.' %
                              config.server.host, log.Level.INFO)
        message.log()

        if config.server.dir:
            try:
                session.cwd(config.server.dir)
            except:
                message = log.Message('%s not exists on %s' % (config.server.dir, config.server.host), log.Level.ERROR)
                message.log()
                return

        filefilter = config.filter
        for path in config.path:
            try:
                uploadfp(session, path, config.server.keep, filefilter, config.delay)
            except:
                pass
        session.close()
        message = log.Message('Upload done!', log.Level.INFO)
        message.log()
        if not config.interval:
            break
        time.sleep(config.interval)


def uploadfp(session, path, keep, filefilter, delay):
    """Upload file.

    Args:
        session:session.
        path:upload path.
        keep:keep directory structure.
        filefilter:file filter.
        delay: delay in seconds.
    """
    if not os.path.isdir(path):
        return
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
    ftpfiles = session.nlst()
    for subpath in os.listdir(path):
        localpath = os.path.join(path, subpath)
        if os.path.isfile(localpath):
            uploadflag = False
            subfile = subpath
            subfilepath = localpath
            if session.zflag in [1, 2]:
                subfile += '.gz'
                subfilepath += '.gz'
            if subfile in ftpfiles and os.path.exists(subfilepath):
                if os.stat(subfilepath).st_size <= session.size(subfile):
                    continue
            if filefilter.pattern:
                for pattern in filefilter.pattern:
                    pfile = PatternFile(pattern, delay)
                    if subpath == pfile.filename:
                        uploadflag = True
            elif filefilter.regex:
                for regex in filefilter.regex:
                    if re.match(r'^%s+$' % regex, subpath):
                        uploadflag = True
                        break
            else:
                if filefilter.rule == 'sgl':
                    if filefilter.prefix:
                        for prefix in filefilter.prefix:
                            if subpath.startswith(prefix):
                                uploadflag = True
                                break
                    if not uploadflag and filefilter.suffix:
                        for suffix in filefilter.suffix:
                            if subpath.endswith(suffix):
                                uploadflag = True
                                break
                elif filefilter.rule == 'com':
                    for prefix, suffix in zip(filefilter.prefix, filefilter.suffix):
                        if subpath.startswith(prefix) and subpath.endswith(suffix):
                            uploadflag = True
                            break
            if not uploadflag:
                continue
            try:
                session.storbinary(localpath)
                message = log.Message('Upload %s successful!' % localpath,
                                      log.Level.INFO)
                message.log()
            except:
                message = log.Message('Upload %s failed!' % localpath,
                                      log.Level.WARNING)
                message.log()
        elif os.path.isdir(localpath):
            uploadfp(session, localpath, keep, filefilter, delay)
            if keep == 'yes':
                session.cwd('..')


def configure(config_path):
    """read configure file."""
    if not os.path.exists(config_path):
        message = log.Message('Not find configure file: %s.' %
                              config_path, log.Level.ERROR)
        message.log()
        return 0

    interval = None
    path = list()
    prefix = list()
    suffix = list()
    rule = None
    regex = list()
    pattern = list()
    delay = 0
    host = None
    user = None
    password = None
    directory = None
    keep = None
    mode = None
    zflag = 0
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
            
            if line.startswith('pattern'):
                pattern.extend(line.split('=')[1].split())
            
            if line.startswith('delay'):
                delay = line.split('=')[1].strip()

            if line.startswith('mode'):
                mode = line.split('=')[1].strip()

            if line.startswith('gz'):
                zflag = line.split('=')[1].strip()

    try:
        mode = int(mode)
    except:
        mode = 0

    try:
        interval = int(interval)
    except:
        interval = 0

    try:
        delay = int(delay)
    except:
        delay = 0
    
    try:
        zflag = int(zflag)
    except:
        zflag = 0
    
    if keep not in ['yes', 'no']:
        keep = 'no'

    config = Config(interval, path, prefix, suffix, rule, regex, pattern, delay, server(
        host, user, password, directory, keep), mode, zflag)
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
