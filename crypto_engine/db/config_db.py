#!/usr/bin/python
from configparser import ConfigParser


def config_db(filename='db/database.ini'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get sections
    db_param = {}
    for section in parser.sections():
        for (key, value) in parser.items(section):
            db_param[key.lower()] = value
    return db_param
