// =====================================================================
// SecondLook - Assignment 3: Cypher queries (Neo4j Aura Free)
// Graph loaded by script/08_load_neo4j.py from the Gold star schema.
// Run one statement at a time in the Aura Query tool (or Neo4j Browser).
// =====================================================================

// =====================================================================
// LOAD: constraints, index and CREATE/MERGE statements
// These are the exact statements 08_load_neo4j.py runs. The script extracts rows from the Gold
// star schema (secondlook_gold) and passes them as $rows in batches of 1,000, so the graph is
// rebuilt from Gold on every run. To run by hand, supply $rows as a parameter list.
// =====================================================================

// Optional reset (the loader deletes in batches of 5,000 to stay within Aura Free memory)
MATCH (n) WITH n LIMIT 5000 DETACH DELETE n RETURN count(*) AS deleted;

CREATE CONSTRAINT customer_sk IF NOT EXISTS FOR (c:Customer) REQUIRE c.customer_sk IS UNIQUE;
CREATE CONSTRAINT article_id IF NOT EXISTS FOR (a:Article) REQUIRE a.article_id IS UNIQUE;
CREATE CONSTRAINT product_type IF NOT EXISTS FOR (t:ProductType) REQUIRE t.name IS UNIQUE;
CREATE CONSTRAINT colour_name IF NOT EXISTS FOR (c:Colour) REQUIRE c.name IS UNIQUE;
CREATE CONSTRAINT pattern_name IF NOT EXISTS FOR (p:Pattern) REQUIRE p.name IS UNIQUE;
CREATE INDEX article_tier IF NOT EXISTS FOR (a:Article) ON (a.sales_tier);

// Customers  ($rows from dim_customer: customer_sk, customer_key, age_band, club_member_status,
//             fashion_news_frequency) -> 3,365 nodes
UNWIND $rows AS r
MERGE (c:Customer {customer_sk: r.customer_sk})
SET c.customer_key = r.customer_key, c.age_band = r.age_band,
    c.club_member_status = r.club_member_status,
    c.fashion_news_frequency = r.fashion_news_frequency;

// Articles + attribute nodes + OF_TYPE / HAS_COLOUR / HAS_PATTERN
//            ($rows from current dim_article rows, with the 2020-06-30 tier from the expired SCD2 row)
//            -> 13,993 Article, 102 ProductType, 49 Colour, 30 Pattern nodes; 3 x 13,993 relationships
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
MERGE (a)-[:HAS_PATTERN]->(p);

// Purchases  ($rows from fact_sales aggregated per customer x article: units, revenue, orders,
//             first_date, last_date) -> 34,004 PURCHASED relationships, 39,030 units
UNWIND $rows AS r
MATCH (c:Customer {customer_sk: r.customer_sk})
MATCH (a:Article {article_id: r.article_id})
MERGE (c)-[p:PURCHASED]->(a)
SET p.units = r.units, p.revenue = r.revenue, p.orders = r.orders,
    p.first_date = date(r.first_date), p.last_date = date(r.last_date);


// ---------------------------------------------------------------------
// 0. Checks and the visualization screenshot
// ---------------------------------------------------------------------
// 0a. Node and relationship counts
MATCH (n) RETURN labels(n)[0] AS label, count(*) AS nodes ORDER BY nodes DESC;

MATCH ()-[r]->() RETURN type(r) AS relationship, count(*) AS relationships ORDER BY relationships DESC;

// 0b. Visualization: a few slow movers, their buyers and their attributes (switch the result to Graph view)
MATCH p = (c:Customer)-[:PURCHASED]->(a:Article {sales_tier: 'Slow Mover'})-[:OF_TYPE|HAS_COLOUR|HAS_PATTERN]->()
RETURN p LIMIT 60;


// =====================================================================
// PART 3: three Cypher queries
// =====================================================================

// ---------------------------------------------------------------------
// Q3.1  What has our most active customer bought, described by type, colour and pattern?
//       (one-hop lookups from a customer to articles and from articles to their attributes)
// ---------------------------------------------------------------------
MATCH (c:Customer)-[:PURCHASED]->(:Article)
WITH c, count(*) AS articles_bought ORDER BY articles_bought DESC LIMIT 1
MATCH (c)-[p:PURCHASED]->(a:Article)-[:OF_TYPE]->(t:ProductType),
      (a)-[:HAS_COLOUR]->(col:Colour),
      (a)-[:HAS_PATTERN]->(pat:Pattern)
RETURN c.customer_sk AS customer, c.age_band AS age_band, a.prod_name AS article,
       t.name AS product_type, col.name AS colour, pat.name AS pattern,
       a.sales_tier AS tier, p.units AS units
ORDER BY units DESC, article
LIMIT 15;

// ---------------------------------------------------------------------
// Q3.2  Which product types hold the most slow movers, and how many distinct customers buy that type?
//       (aggregation across a type's articles and their buyers)
// ---------------------------------------------------------------------
MATCH (t:ProductType)<-[:OF_TYPE]-(a:Article)
OPTIONAL MATCH (a)<-[p:PURCHASED]-(c:Customer)
RETURN t.name AS product_type,
       count(DISTINCT a)                                                    AS articles,
       count(DISTINCT CASE WHEN a.sales_tier = 'Slow Mover' THEN a END)     AS slow_movers,
       count(DISTINCT CASE WHEN a.sales_tier = 'Never Sold' THEN a END)     AS never_sold,
       count(DISTINCT c)                                                    AS distinct_buyers,
       coalesce(sum(p.units), 0)                                            AS units
ORDER BY slow_movers DESC
LIMIT 10;

// ---------------------------------------------------------------------
// Q3.3  MULTI-HOP: for a slow mover, which customers bought other articles of the SAME product type
//       AND the SAME colour, but have never bought this one?  (SecondLook's targeting list)
//       Path: slow mover -> type <- similar article -> colour <- slow mover, then similar article <- customer
//       In SQL this needs the article table joined to itself on two attributes, then to sales and
//       customers, plus an anti-join; here it is one pattern.
// ---------------------------------------------------------------------
MATCH (x:Article {sales_tier: 'Slow Mover'})-[:OF_TYPE]->(t:ProductType)
WITH x, t, COUNT { (t)<-[:OF_TYPE]-(:Article) } AS type_size
ORDER BY type_size DESC, x.article_id
LIMIT 1
MATCH (x)-[:HAS_COLOUR]->(col:Colour)
MATCH (t)<-[:OF_TYPE]-(sim:Article)-[:HAS_COLOUR]->(col)
WHERE sim <> x
MATCH (c:Customer)-[p:PURCHASED]->(sim)
WHERE NOT EXISTS { MATCH (c)-[:PURCHASED]->(x) }
WITH x, t, col, c, sum(p.units) AS matching_units, count(DISTINCT sim) AS matching_articles
RETURN x.article_id AS slow_mover, x.prod_name AS name, t.name AS product_type, col.name AS colour,
       c.customer_sk AS target_customer, c.age_band AS age_band, matching_articles, matching_units
ORDER BY matching_units DESC, matching_articles DESC
LIMIT 20;


// =====================================================================
// PART 4b: three queries that play to the graph's strengths
// =====================================================================

// ---------------------------------------------------------------------
// Q4.1  "Customers who bought this also bought": for articles that RECOVERED from Slow Mover to Normal,
//       which other articles do their buyers share most, and in which colours?  (2-hop co-purchase traversal)
// ---------------------------------------------------------------------
MATCH (x:Article {tier_2020_06_30: 'Slow Mover', sales_tier: 'Normal'})<-[:PURCHASED]-(c:Customer)
      -[:PURCHASED]->(other:Article)
WHERE other <> x
MATCH (x)-[:HAS_COLOUR]->(xc:Colour)
MATCH (other)-[:HAS_COLOUR]->(oc:Colour)
RETURN x.prod_name AS recovered_article, xc.name AS recovered_colour,
       other.prod_name AS also_bought, oc.name AS also_bought_colour,
       other.sales_tier AS also_bought_tier, count(DISTINCT c) AS shared_customers
ORDER BY shared_customers DESC, recovered_article
LIMIT 15;

// ---------------------------------------------------------------------
// Q4.2  Customer similarity: which customers share at least 3 product types with our most active
//       customer?  (4-hop path customer -> article -> type <- article <- customer; in SQL this is
//       four joins plus a self-join on customers)
// ---------------------------------------------------------------------
MATCH (a:Customer)-[:PURCHASED]->(:Article)
WITH a, count(*) AS n ORDER BY n DESC LIMIT 1
MATCH (a)-[:PURCHASED]->(:Article)-[:OF_TYPE]->(t:ProductType)
WITH a, collect(DISTINCT t) AS types
UNWIND types AS t
MATCH (t)<-[:OF_TYPE]-(:Article)<-[:PURCHASED]-(b:Customer)
WHERE b <> a
WITH a, size(types) AS types_of_a, b, count(DISTINCT t) AS shared_types
WHERE shared_types >= 3
RETURN a.customer_sk AS customer, types_of_a, b.customer_sk AS similar_customer,
       b.age_band AS age_band, shared_types
ORDER BY shared_types DESC, similar_customer
LIMIT 15;

// ---------------------------------------------------------------------
// Q4.3  Path finding: how many "shared purchase" steps connect two customers?
//       (variable-length shortestPath over PURCHASED only; SQL would need a recursive CTE)
//       Switch to Graph view to see the chain.
// ---------------------------------------------------------------------
MATCH (a:Customer {customer_sk: 1})
MATCH (b:Customer {customer_sk: 2})
MATCH p = shortestPath((a)-[:PURCHASED*..8]-(b))
RETURN p, length(p) AS hops, [n IN nodes(p) WHERE n:Article | n.prod_name] AS articles_on_path;
