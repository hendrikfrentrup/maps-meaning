import pyspark
import pyspark.sql.functions as F
import graphframes as gf
from functools import reduce

def create_mirror(df):
    mirrorColNames = [f"_{col}" for col in df.columns]
    return df.toDF(*mirrorColNames)

def generate_edges(nodes, match_on):
    for c in match_on:
        if c not in nodes.columns:
            raise ValueError('match_on colsumns not in DataFrame')
    if "id" in nodes.columns:
        mirror = create_mirror(nodes)
        match_conditions = [F.col(c) == F.col(f'_{c}') for c in match_on]
        conditions = [(F.col("id") != F.col("_id"))
                & reduce(lambda x, y: x | y, match_conditions)]
        edges = nodes.join(mirror, conditions).select("id", "_id")\
                        .withColumnRenamed('id', 'src')\
                        .withColumnRenamed('_id', 'dst')
        return edges
    else:
        raise ValueError('Id column missing in nodes DataFrame')


def generate_nodes(df):
    id_col = [c for c in df.columns if "id" in c]
    if len(id_col)==1:
        return df.withColumnRenamed(id_col[0], 'id')
    else:
        raise ValueError('Id column missing in DataFrame')

def resolve(sc, nodes, edges):
    graph = gf.GraphFrame(nodes, edges)
    sc.setCheckpointDir("/tmp/checkpoints")
    return graph.connectedComponents()
