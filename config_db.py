#!/usr/bin/python
from configparser import ConfigParser


def config(filename='database.ini'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get sections
    db = {}
    for section in parser.sections():
        for (key, value) in parser.items(section):
            db[key.lower()] = value

    return db