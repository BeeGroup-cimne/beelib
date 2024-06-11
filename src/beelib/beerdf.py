from neo4j import GraphDatabase
from rdflib import graph, RDF
import rdflib


def __get_namespaced_fields__(field, context):
    split_type = field.split("__")
    if len(split_type) > 2 or len(split_type) < 2:
        return None
    else:
        return context[split_type[0]][split_type[1]]


def create_rdf_from_neo4j(neo4j_graph, context):
    context_ns = {}
    for c in context.nodes:
        for k, v in c.items():
            prefix_tmp = rdflib.Namespace(v)
            context_ns[k] = prefix_tmp

    g = graph.Graph()
    # create the nodes
    for n in neo4j_graph.nodes:
        # get the subject
        try:
            subject = rdflib.URIRef(n.get("uri"))
        except:
            continue
        # set the type of the class
        for k in [t for t in n.labels if t != "Resource"]:
            lab = __get_namespaced_fields__(k, context_ns)
            if not lab:
                continue
            g.add((subject, RDF.type, lab))
        # set the data properties
        for k in [p for p in n.keys() if p != "uri" and "@" not in p]:
            lab = __get_namespaced_fields__(k, context_ns)
            if not lab:
                continue
            item = n.get(k)
            if isinstance(item, list):
                for item_val in item:
                    if isinstance(item_val, str):
                        text = item_val.split("@")[0]
                        try:
                            lang = item_val.split("@")[1]
                            v = rdflib.Literal(text, lang=lang)
                        except:
                            v = rdflib.Literal(text)
                    else:
                        v = rdflib.Literal(item_val)
                    g.add((subject, lab, v))
            else:
                v = rdflib.Literal(item)
                g.add((subject, lab, v))

    for r in neo4j_graph.relationships:
        rel_type = __get_namespaced_fields__(r.type, context_ns)
        if not rel_type:
            continue
        try:
            subject_orig = rdflib.URIRef(r.start_node.get('uri'))
            subject_end = rdflib.URIRef(r.end_node.get('uri'))
            g.add((subject_orig, rel_type, subject_end))
        except:
            pass
    for k, v in context_ns.items():
        g.bind(k, v)

    return g


def get_rdf_with_cyper_query(query, connection):
    driver = GraphDatabase.driver(**connection)
    with driver.session() as session:
        users = session.run(
            query
        ).graph()
        context = session.run(
            f"""MATCH (n:`_NsPrefDef`) RETURN n"""
        ).graph()
    return create_rdf_from_neo4j(neo4j_graph=users, context=context)


def serialize_with_cyper_query(query, connection, format):
    g = get_rdf_with_cyper_query(query, connection)
    return g.serialize(format=format)
