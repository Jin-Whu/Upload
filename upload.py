#!/usr/bin/env python
# coding:utf-8

import argparse
import ftplib
import log
import os
import time
import re
from collections import namedtuple
import paramiko
from paramiko import SSHClient
from paramiko import SFTPClient


server = namedtuple('SERVER', ('host', 'user', 'password', 'dir', 'keep'))
filefilter = namedtuple('Filter', ('prefix', 'suffix', 'rule', 'regex'))

class Config(object):
    """configure value.

    Attributes:
        interlval:interval to upload, in seconds, type:int.
        path:file path to upload, type:list.
        prefix: prefix, type:list.
        suffix: suffix, type:list;
        rule: combine or single, type:str.
        regex: regex, type:list.
        server:ftp or ssh server, type:namedtuple.
        filter: filter rule, type:namedtuple.
    """

    def __init__(self, interval, path, prefix, suffix, rule, regex, server, mode):
        self.interval = interval
        self.path = path
        self.prefix = prefix
        self.suffix = suffix
        self.rule = rule
        self.regex = regex
        self.server = server
        self.mode = mode
        self.filter = None

    def check(self):
        """check interval and path if valid."""
        if not self.path:
            message = log.Message('No path upload!', log.Level.ERROR)
            message.log()
            return 0

        for filepath in self.path:
            if not os.path.exists(filepath):
                message = log.Message('Not find path to upload: %s.' % filepath,
                                      log.Level.WARNING)
                message.log()
                # return 0 (enable non-existed path)

        if not self.server.host or not self.server.user or not self.server.password:
            message = log.Message('Missing ftp information.', log.Level.ERROR)
            message.log()
            return 0

        if not self.regex:
            if self.rule not in ['sgl', 'com']:
                message = log.Message('rule must be sgl or com.', log.Level.ERROR)
                message.log()
                return 0

        if self.server.keep not in ['yes', 'no']:
            message = log.Message('ftp keep must be yes or no.', log.Level.ERROR)
            message.log()
            return 0
        
        if self.mode not in [0, 1]:
            message = log.Message('mode must be 0[ftp] or 1[sftp].', log.Level.ERROR)
            message.log()
            return 0

        self.filter = filefilter(self.prefix, self.suffix, self.rule, self.regex)

        return 1


class Session(object):
    def __init__(self, server, mode):
        self.server = server
        self.mode = mode
        self.session = None
    
    def connect(self):
        host = self.server.host.split(':')
        port = 21 if self.mode == 0 else 22
        if len(host) > 1:
            try:
                port = int(host[1])
            except:
                message = log.Message('port: %s is invalid.' % host[1], log.Level.ERROR)
                return False
        
        if self.mode == 0:
            self.session = ftplib.FTP()
            self.session.connect(host[0], port)
            self.session.login(self.server.user, self.server.password)
        else:
            ssh = SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(host[0], port, username=self.server.user, password=self.server.password)
            self.session = SFTPClient.from_transport(ssh.get_transport())
        return True
    

    def close(self):
        if self.session:
            if self.mode == 0:
                self.session.quit()
            elif self.mode == 1:
                self.session.close()
    
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
    
    def storbinary(self, filepath, fp=None, blocksize=1024):
        filename = os.path.basename(filepath)
        if self.session:
            if self.mode == 0:
                self.session.storbinary('STOR %s' % filename, fp, blocksize)
            elif self.mode == 1:
                self.session.put(filepath, filename)


def upload(config):
    """Upload file using ftp."""
    while True:
        session = Session(config.server, config.mode)
        session.connect()
        message = log.Message('Login to %s.' % config.server.host, log.Level.INFO)
        message.log()

        if config.server.dir:
            try:
                session.cwd(config.server.dir)
            except:
                pass
        filefilter = config.filter
        for path in config.path:
            try:
                uploadfp(session, path, config.server.keep, filefilter)
            except:
                pass
        try:
            session.close()
        except:
            pass
        message = log.Message('Upload done!', log.Level.INFO)
        message.log()
        if not config.interval:
            break
        time.sleep(config.interval)


def uploadfp(session, path, keep, filefilter):
    """Upload file.

    Args:
        session:session.
        path:upload path.
        keep:keep directory structure.
        filefilter:file filter.
    """
    if not os.path.isdir(path):
        return
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
    ftpfiles = session.nlst()
    for subpath in os.listdir(path):
        localpath = os.path.join(path, subpath)
        if os.path.isfile(localpath):
            if subpath in ftpfiles:
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
                    session.storbinary(localpath, f, 1024)
                message = log.Message('Upload %s successful!' % localpath,
                                      log.Level.INFO)
                message.log()
            except:
                message = log.Message('Upload %s failed!' % localpath,
                                      log.Level.WARNING)
                message.log()
                raise
        elif os.path.isdir(localpath):
            uploadfp(session, localpath, keep, filefilter)
            if keep == 'yes':
                session.cwd('..')


def configure(config_path):
    """read configure file."""
    if not os.path.exists(config_path):
        message = log.Message('Not find configure file: %s.' % config_path, log.Level.ERROR)
        message.log()
        return 0

    interval = None
    path = list()
    prefix = list()
    suffix = list()
    rule = None
    regex = list()
    host = None
    user = None
    password = None
    directory = None
    keep = None
    mode = None
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

            if line.startswith('mode'):
                mode = line.split('=')[1].strip()

    try:
        interval = int(interval)
    except:
        message = log.Message('interval: %s is invalid.' % interval, log.Level.ERROR)
        message.log()
        return 0

    try:
        mode = int(mode)
    except:
        message = log.Message('mode: %s is invalid.' % mode, log.Level.ERROR)
        message.log()
        return 0

    config = Config(interval, path, prefix, suffix, rule, regex, server(host, user, password, directory, keep), mode)
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
