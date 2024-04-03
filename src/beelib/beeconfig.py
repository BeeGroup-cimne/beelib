import json
import os


def read_config(conf_file=None):
    if conf_file:
        conf = json.load(open(conf_file))
    else:
        conf = json.load(open(os.environ['CONF_FILE']))
    if 'neo4j' in conf and 'auth' in conf['neo4j']:
        conf['neo4j']['auth'] = tuple(conf['neo4j']['auth'])
    return conf
