import json
import os
import re


def read_config(conf_file=None):
    if conf_file:
        conf = json.load(open(conf_file))
    else:
        conf = json.load(open(os.environ['CONF_FILE']))
    for k in [re.match(r'neo4j.*', k).string for k in conf.keys() if re.match(r'neo4j.*', k) is not None]:
        if 'auth' in conf[k]:
            conf[k]['auth'] = tuple(conf[k]['auth'])
    return conf
