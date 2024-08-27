import hashlib
import tempfile
from urllib.parse import quote

from neo4j import GraphDatabase
import json
import morph_kgc
import os


def __map_to_ttl__(data, mapping_file):
    morph_config = "[DataSource1]\nmappings:{mapping_file}\nfile_path: {d_file}"
    with tempfile.NamedTemporaryFile(mode='w', delete=False, dir=".", suffix=".json") as d_file:
        json.dump({k: v for k, v in data.items()}, d_file)
    g_rdflib = morph_kgc.materialize(morph_config.format(mapping_file=mapping_file, d_file=d_file.name))
    os.unlink(d_file.name)
    return g_rdflib


def __transform_to_str__(graph):
    content = graph.serialize(format="ttl")
    content = content.replace('\\"', "&apos;")
    content = content.replace("'", "&apos;")
    return content


def map_and_save(data, mapping_file, config):
    g = __map_to_ttl__(data, mapping_file)
    save_to_neo4j(g, config)


def map_and_print(data, mapping_file, config):
    g = __map_to_ttl__(data, mapping_file)
    print_graph(g, config)


def save_to_neo4j(g, config):
    content = __transform_to_str__(g)
    neo = GraphDatabase.driver(**config['neo4j'])
    with neo.session() as s:
        response = s.run(f"""CALL n10s.rdf.import.inline('{content}','Turtle')""")
        print(response.single())


def print_graph(g, config):
    content = __transform_to_str__(g)
    if 'print_file' in config and config['print_file']:
        with open(config['print_file'], "w") as f:
            f.write(content)
    else:
        print(g.serialize(format="ttl"))


def create_hash(uri):
    uri = quote(uri, safe=':/#')
    uri = uri.encode()
    m = hashlib.sha256(uri)
    return m.hexdigest()
