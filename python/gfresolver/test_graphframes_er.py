import pytest
from gfresolver.bootstrap_spark import get_spark
from gfresolver.gfresolver import create_mirror, \
                                  generate_nodes, \
                                  generate_edges, \
                                  resolve


@pytest.fixture
def init_spark():
    return get_spark()
    
@pytest.fixture
def test_df(init_spark):
    _, spark = init_spark
    return spark.read.option("header", "true").csv("data/small_test_data.csv")

def test_create_mirror(test_df):
    mirror_df = create_mirror(test_df)
    mirror_cols = mirror_df.columns
    for col in test_df.columns:
        assert '_'+col in mirror_cols 

def test_generate_nodes(init_spark, test_df):
    _, spark = init_spark

    df = spark.createDataFrame(
            [('a', 1, None), 
            ('b', 1, 1)], 
            schema=('bla', 'foo', 'bar')
            )

    with pytest.raises(ValueError):
        generate_nodes(df)
    
    nodes = generate_nodes(test_df)
    assert 'id' in nodes.columns


def test_generate_edges(test_df):
    print(test_df.columns)
    nodes = generate_nodes(test_df)

    with pytest.raises(ValueError):
        generate_edges(test_df, ["ip", "mac", "hostname"])
    
    with pytest.raises(ValueError):
        generate_edges(test_df, ["columd_not_present"])

    edges = generate_edges(nodes, ["ip", "mac", "hostname"])
    assert edges.columns == ['src', 'dst']

    adjencents = edges.groupBy('src').count().collect()
    for r in adjencents:
        assert r['count']==2

def test_resolve(init_spark, test_df):
    sc, _ = init_spark
    nodes = generate_nodes(test_df)
    edges = generate_edges(nodes, ["ip", "mac", "hostname"])
    cc = resolve(sc, nodes, edges)
    assert cc.groupBy("component").count()\
             .orderBy('count', ascending=False)\
             .first()["count"] == 3

