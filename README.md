# Beelib

This library includes tools and functions divided in different subpackages to perform common actions such 
as connecting to ENMA databases, transforming data to knowledge graph in RDF and upload them to Neo4j

# installation

The best way to install the package is using the github repository, where we can choose the version, 
or get the last one 

```bash
pip install git+https://github.com/BeeGroup-cimne/beelib[@<version>]
```

# Packages and utilities

##### beeconfig
Submodule used to read a configuration file with secrets and information used in beegroup

- *read_config(conf_file=None)*

  function used to read the a config_file 
  - *conf_file:* the `config.json` file to read, if None is passed, the file pointed 
    by the environment variable named "CONF_FILE" will be read instead

##### beehbase

Submodule used to access and store hbase information

- *save_to_hbase(documents, h_table_name, hbase_connection, cf_mapping, row_fields=None, batch_size=1000)*

  function used to write data to hbase
    - *documents:* the documents (list of dictionaries) to store in the database
    - *h_table_name:* the hbase table name to store the data
    - *hbase_connection:* the connection to the hbase 
    - *cf_mapping:* the information of which data key must go to which column_families to use 
      ex. `[("cf", "dict_key")]` where `cf` is the column family name of the table and `dict_key` can be each key in
      the data dictionary or `all` to store all keys in the same column family 
    - *row_fields:* list of `dictionary_keys` in the `documents` that will be used as `row`in hbase table. The order 
      indicates also the priority in the key ex. ["k1","k2","k3"] will generate `k1_d~k2_d~k3_d`
    - *batch_size:* the data storing will be made in batches of this size of elements each time to improve performance

- *get_hbase_data_batch(hbase_conf, hbase_table, row_start=None, row_stop=None, row_prefix=None, columns=None,
                         _filter=None, timestamp=None, include_timestamp=False, batch_size=100000,
                         scan_batching=None, limit=None, sorted_columns=False, reverse=False)*

  function used to read data from hbase
  - *hbase_conf:*  the connection to the hbase 
  - *hbase_table:* the hbase table name to store the data
  - *row_start:* the row to start the scanning of the table
  - *row_stop:* the row to stop the scanning of the table
  - *row_prefix:* the prefix to use for scanning the table
  - *columns:* the columns to retrieve
  - *_filter:* query to filter rows
  - *timestamp:* filter from timestamp cells 
  - *include_timestamp:* boolean to include or not the timestamp_field
  - *batch_size*: the scan will be performed returning as many records each time
  - *scan_batching:* unused, 
  - *limit*: limit the number of rows to retrieve
  - *sorted_columns*: if the columns will be sorted in the results
  - *reverse*: perform the scan in the reverse way

##### beekafka
Submodule used to send data to kafka

- *create_kafka_producer(config, retry_kwargs=None)*

  creates a new kafka producer
    - *config:* the configuration dict with a "kafka" document containing the connection information
    - *retry_kwargs:* optional dict overriding the retry defaults (see `beeretry`) for the initial
      connection check; if not given, the defaults are used

- *send_to_kafka(producer, topic, key, data, \*\*kwargs)*

  sends the information to kafka creating a common message with metadata
    - *producer:* the producer to use to send the data
    - *topic:* The topic to send the data to
    - *key:* The key to use for the message(can be None)
    - *data:* the data list to send to kafka
    - *kwargs:* any parameter to use as metadata

##### beeretry
Submodule with the retry-with-backoff mechanism shared by the connection helpers in `beeredis`, 
`beeneo4j`, `beecassandra`, `beeinflux`, `beekafka` and `beerest`

- *retry_with_backoff(fn, \*args, exceptions=Exception, attempts=10, base_delay=5, max_delay=60, factor=2, jitter=0.2, on_retry=None, \*\*kwargs)*

  calls `fn(*args, **kwargs)`, retrying with exponential backoff (capped at `max_delay`, plus jitter) 
  if it raises one of `exceptions`, up to `attempts` tries
    - *attempts:* number of tries before giving up (default 10)
    - *base_delay:* initial wait in seconds before the first retry (default 5)
    - *max_delay:* cap in seconds for the wait between retries (default 60)
    - *factor:* multiplier applied to the delay after each failed attempt (default 2)
    - *jitter:* random fraction added to each wait so several connections failing at once don't 
      retry in lockstep (default 0.2)
    - *on_retry:* optional `fn(attempt, exception)` callback invoked before each wait

  Every connection helper below already retries with these defaults, and lets you override them 
  for that specific connection by passing the same parameters. If none are passed, the defaults 
  above apply:
    - *beeredis.connect_redis(cfg, \*\*retry_kwargs)*
    - *beeneo4j.connect_neo4j(config, \*\*retry_kwargs)*
    - *beecassandra.get_session(cassandra_connection, \*\*retry_kwargs)* and 
      *beecassandra.get_cassandra_data(..., \*\*retry_kwargs)*
    - *beeinflux.run_query(influx_connection, query, \*\*retry_kwargs)* and 
      *beeinflux.get_timeseries_by_hash(..., \*\*retry_kwargs)*
    - *beekafka.create_kafka_producer(config, retry_kwargs=None, ...)* and 
      *beekafka.create_kafka_consumer(config, retry_kwargs=None, ...)*
    - *beerest.session_with_retries(exceptions=..., \*\*retry_kwargs)*

##### beesecurity
Submodule used to encrypt passwords with symmetrical encryption using AES-256.

- *encrypt(plain_text, password)*

  encrypts the text with the secret password.  
  - *plain_text:* the text to encrypt
  - *password:* the password to use for encryption


- *decrypt(enc_str, password)*
  
  decrypts the enc_str to get the plain text
    - *enc_str:* The encrypted string
    - *password:* The password to decrypt the text
  
##### beetransformation
Submodule to transform data to RDF and store it into neo4j
  
- *map_and_save(data, mapping_file, config)*
  
   transforms data to RDF and saves it in neo4j
    - *data:* The data to transform
    - *mapping_file:* yaml using the yarrrml transformation language
    - *config:* the config file with a `neo4j` key to access the database


- *map_and_print(data, mapping_file, config)*
  
  transforms data to RDF and prints it in the standard output or file
    - *data:* The data to transform
    - *mapping_file:* yaml using the yarrrml transformation language
    - *config:* the config file, can be an empty dict {} used only for mimetizing with `map_and_save` 
       or contain a `print_file` field to store the RDF in a file instead

-  *save_to_neo4j(g, config)*
  
   stores an already created RDF graph to neo4j
    - *g*: a RDF graph in `rdflib` object
    - *config:* the config file with a `neo4j` key to access the database

- *print_graph(g, config)*

  prints an already created RDF graph in the standard output or file
    - *g*: a RDF graph in `rdflib` object
    - *config:* the config file, can be an empty dict {} used only for mimetizing with `map_and_save` 
      or contain a `print_file` field to store the RDF in a file instead

## Authors

- Arian Huapaya - ahuapaya@cimne.upc.edu
- Núria Salvador - nsalvador@cimne.upc.edu
- Eloi Gabaldón - egabaldon@cimne.upc.edu
- Jordi Carbonell - jordi@cimne.upc.edu

Copyright (c) 2026 Arian Huapaya, Núria Salvador, Eloi Gabaldon, Jordi Carbonell
