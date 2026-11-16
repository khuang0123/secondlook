"""
Assignment 3, Part 3: load the SecondLook property graph into Neo4j Aura Free.

Layer rule: reads ONLY from the Gold star schema (secondlook_gold) in MySQL.

Graph model
  Nodes
    (:Customer     {customer_sk, customer_key, age_band, club_member_status, fashion_news_frequency})
    (:Article      {article_id, prod_name, product_group, index_group, department,
                    sales_tier, tier_2020_06_30, tier_changed})
    (:ProductType  {name, product_group})
    (:Colour       {name})
    (:Pattern      {name})                       graphical appearance: Solid, Stripe, Denim, ...
  Relationships
    (:Customer)-[:PURCHASED {units, revenue, orders, first_date, last_date}]->(:Article)
    (:Article)-[:OF_TYPE]->(:ProductType)
    (:Article)-[:HAS_COLOUR]->(:Colour)
    (:Article)-[:HAS_PATTERN]->(:Pattern)
  PURCHASED is aggregated to one relationship per customer x article (from fact_sales).
  Customers carry only the masked key from Gold; no raw identifiers reach the graph.

Setup
  pip install neo4j pandas sqlalchemy pymysql
  export MYSQL_HOST=<rds-endpoint> MYSQL_USER=<user>
  export NEO4J_URI=neo4j+s://<instance-id>.databases.neo4j.io  NEO4J_USER=neo4j
  (MySQL and Neo4j passwords are prompted, or read from MYSQL_PASSWORD / NEO4J_PASSWORD)
Run
  python script/08_load_neo4j.py            # rebuilds the graph from scratch (safe to re-run)
"""
import os
from getpass import getpass
from urllib.parse import quote_plus

import pandas as pd
from neo4j import GraphDatabase
from sqlalchemy import create_engine

GOLD = os.environ.get("MYSQL_GOLD_DB", "secondlook_gold")
BATCH = 1000


# ------------------------------------------------------------------ 1. extract from Gold
def gold_engine():
    pw = os.environ.get("MYSQL_PASSWORD") or getpass("MySQL password: ")
    sock = f"?unix_socket={os.environ['MYSQL_SOCKET']}" if os.environ.get("MYSQL_SOCKET") else ""
    return create_engine(f"mysql+pymysql://{os.environ['MYSQL_USER']}:{quote_plus(pw)}"
                         f"@{os.environ['MYSQL_HOST']}:{os.environ.get('MYSQL_PORT', '3306')}/{GOLD}{sock}")


def extract(eng):
    customers = pd.read_sql("""
        SELECT customer_sk, customer_key, age_band, club_member_status, fashion_news_frequency
        FROM dim_customer""", eng)
    articles = pd.read_sql("""
        SELECT cur.article_id, cur.prod_name,
               COALESCE(cur.product_type_name, 'Unknown')         AS product_type_name,
               cur.product_group_name,
               COALESCE(cur.colour_group_name, 'Unknown')         AS colour_group_name,
               COALESCE(cur.graphical_appearance_name, 'Unknown') AS graphical_appearance_name,
               cur.index_group_name,
               cur.department_name, cur.sales_tier,
               COALESCE(old.sales_tier, cur.sales_tier) AS tier_2020_06_30,
               (old.article_sk IS NOT NULL)             AS tier_changed
        FROM dim_article cur
        LEFT JOIN dim_article old ON old.article_id = cur.article_id AND old.is_current = FALSE
        WHERE cur.is_current = TRUE""", eng)
    purchases = pd.read_sql("""
        SELECT f.customer_sk, a.article_id,
               SUM(f.quantity)                          AS units,
               SUM(f.revenue)                           AS revenue,
               COUNT(DISTINCT f.order_id)               AS orders,
               MIN(d.full_date)                         AS first_date,
               MAX(d.full_date)                         AS last_date
        FROM fact_sales f
        JOIN dim_article a ON a.article_sk = f.article_sk
        JOIN dim_date d    ON d.date_key = f.date_key
        GROUP BY f.customer_sk, a.article_id""", eng)
    articles["tier_changed"] = articles["tier_changed"].astype(bool)
    purchases["revenue"] = purchases["revenue"].astype(float)
    purchases["units"] = purchases["units"].astype(int)
    for col in ["first_date", "last_date"]:                      # ISO strings -> Cypher date()
        purchases[col] = pd.to_datetime(purchases[col]).dt.strftime("%Y-%m-%d")
    print(f"extracted from {GOLD}: {len(customers):,} customers, {len(articles):,} articles, "
          f"{len(purchases):,} customer-article purchases ({int(purchases['units'].sum()):,} units)")
    return customers, articles, purchases


# ------------------------------------------------------------------ 2. load into Neo4j
CONSTRAINTS = [
    "CREATE CONSTRAINT customer_sk IF NOT EXISTS FOR (c:Customer) REQUIRE c.customer_sk IS UNIQUE",
    "CREATE CONSTRAINT article_id IF NOT EXISTS FOR (a:Article) REQUIRE a.article_id IS UNIQUE",
    "CREATE CONSTRAINT product_type IF NOT EXISTS FOR (t:ProductType) REQUIRE t.name IS UNIQUE",
    "CREATE CONSTRAINT colour_name IF NOT EXISTS FOR (c:Colour) REQUIRE c.name IS UNIQUE",
    "CREATE CONSTRAINT pattern_name IF NOT EXISTS FOR (p:Pattern) REQUIRE p.name IS UNIQUE",
    "CREATE INDEX article_tier IF NOT EXISTS FOR (a:Article) ON (a.sales_tier)",
]

LOAD = {
    "customers": """
        UNWIND $rows AS r
        MERGE (c:Customer {customer_sk: r.customer_sk})
        SET c.customer_key = r.customer_key, c.age_band = r.age_band,
            c.club_member_status = r.club_member_status,
            c.fashion_news_frequency = r.fashion_news_frequency""",
    "articles": """
        UNWIND $rows AS r
        MERGE (a:Article {article_id: r.article_id})
        SET a.prod_name = r.prod_name, a.product_group = r.product_group_name,
            a.index_group = r.index_group_name, a.department = r.department_name,
            a.sales_tier = r.sales_tier, a.tier_2020_06_30 = r.tier_2020_06_30,
            a.tier_changed = r.tier_changed
        MERGE (t:ProductType {name: r.product_type_name}) SET t.product_group = r.product_group_name
        MERGE (col:Colour {name: r.colour_group_name})
        MERGE (p:Pattern {name: r.graphical_appearance_name})
        MERGE (a)-[:OF_TYPE]->(t)
        MERGE (a)-[:HAS_COLOUR]->(col)
        MERGE (a)-[:HAS_PATTERN]->(p)""",
    "purchases": """
        UNWIND $rows AS r
        MATCH (c:Customer {customer_sk: r.customer_sk})
        MATCH (a:Article {article_id: r.article_id})
        MERGE (c)-[p:PURCHASED]->(a)
        SET p.units = r.units, p.revenue = r.revenue, p.orders = r.orders,
            p.first_date = date(r.first_date), p.last_date = date(r.last_date)""",
}


def batches(df):
    records = df.astype(object).where(df.notna(), None).to_dict("records")
    for i in range(0, len(records), BATCH):
        yield records[i:i + BATCH]


def clear_graph(session):
    """Delete everything in small batches so re-runs start clean (Aura Free has limited memory)."""
    deleted = 1
    while deleted:
        deleted = session.run("MATCH (n) WITH n LIMIT 5000 DETACH DELETE n RETURN count(*) AS n").single()["n"]


def main():
    customers, articles, purchases = extract(gold_engine())

    uri = os.environ["NEO4J_URI"]
    auth = (os.environ.get("NEO4J_USER", "neo4j"), os.environ.get("NEO4J_PASSWORD") or getpass("Neo4j password: "))
    with GraphDatabase.driver(uri, auth=auth) as driver:
        driver.verify_connectivity()
        with driver.session() as session:
            print("clearing existing graph ...")
            clear_graph(session)
            for stmt in CONSTRAINTS:
                session.run(stmt)
            for name, df in [("customers", customers), ("articles", articles), ("purchases", purchases)]:
                for rows in batches(df):
                    session.execute_write(lambda tx, q=LOAD[name], r=rows: tx.run(q, rows=r).consume())
                print(f"  loaded {name:<10} {len(df):>7,} rows")

            print("\n=== GRAPH COUNTS (compare with Gold) ===")
            for label in ["Customer", "Article", "ProductType", "Colour", "Pattern"]:
                n = session.run(f"MATCH (n:{label}) RETURN count(n) AS n").single()["n"]
                print(f"  (:{label:<12}) {n:>7,}")
            for rel in ["PURCHASED", "OF_TYPE", "HAS_COLOUR", "HAS_PATTERN"]:
                n = session.run(f"MATCH ()-[r:{rel}]->() RETURN count(r) AS n").single()["n"]
                print(f"  [:{rel:<12}] {n:>7,}")
            units = session.run("MATCH ()-[p:PURCHASED]->() RETURN sum(p.units) AS u").single()["u"]
            print(f"  total PURCHASED units: {units:,}  (Gold fact_sales: {int(purchases['units'].sum()):,})")


if __name__ == "__main__":
    main()
