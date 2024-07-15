# -*- coding: utf-8 -*-

'''
@CreateTime: 2024/07/15 10:57:07
@Author: YQ
@Description: 功能的实现描述 
@URL: https://github.com/johnymontana/graph-of-thrones/blob/master/network-of-thrones.ipynb
@UpdateTime: 
@To_do: 下一步开发计划
'''

from py2neo import Graph
from igraph import Graph as IGraph

graph = Graph()

# ##Import into Neo4j
# 唯一约束
graph.run("CREATE CONSTRAINT IF NOT EXISTS FOR (c:Character) REQUIRE c.name IS UNIQUE")

# origin url: https://github.com/pupimvictor/NetworkOfThrones/blob/master/stormofswords.csv
# Download the file to the corresponding directory folder of neo4j's import.
for record in graph.run('''
LOAD CSV WITH HEADERS FROM "file:/stormofswords.csv" AS row
MERGE (src:Character {name: row.Source})
MERGE (tgt:Character {name: row.Target})
MERGE (src)-[r:INTERACTS]->(tgt)
SET r.weight = toInteger(row.Weight)
RETURN count(*) AS paths_written
'''):
    print(record)

for r in graph.run('''
MATCH p=(:Character)-[:INTERACTS]-(:Character)
RETURN p
'''):
    print(r)

# ##Analyzing the network
# Number of characters
for record in graph.run("MATCH (c:Character) RETURN count(c) AS num"):
    print(record)
# Summary statistics
for record in graph.run('''
MATCH (c:Character)-[:INTERACTS]->()
WITH c, count(*) AS num
RETURN min(num) AS min, max(num) AS max, avg(num) AS avg_characters, stdev(num) AS stdev
'''):
    print(record)
# Diameter of the network
# old extract(x IN nodes(p) | x.name)
# new [x IN list | x.prop]
for r in graph.run('''
// Find maximum diameter of network
// maximum shortest path between two nodes
MATCH (a:Character), (b:Character) WHERE id(a) > id(b)
MATCH p=shortestPath((a)-[:INTERACTS*]-(b))
RETURN length(p) AS len, [x IN nodes(p) | x.name] AS path
ORDER BY len DESC LIMIT 4
'''):
    print(r)

# Shortest path
for r in graph.run('''
// Shortest path from Catelyn Stark to Khal Drogo
MATCH (catelyn:Character {name: "Catelyn"}), (drogo:Character {name: "Drogo"})
MATCH p=shortestPath((catelyn)-[INTERACTS*]-(drogo))
RETURN p
'''):
    print(r)

# All shortest paths
for r in graph.run('''
// All shortest paths from Catelyn Stark to Khal Drogo
MATCH (catelyn:Character {name: "Catelyn"}), (drogo:Character {name: "Drogo"})
MATCH p=allShortestPaths((catelyn)-[INTERACTS*]-(drogo))
RETURN p
'''):
    print(r)

# Pivotal nodes
for r in graph.run('''
// Find all pivotal nodes in network
MATCH (a:Character), (b:Character) WHERE id(a) > id(b)
MATCH p=allShortestPaths((a)-[:INTERACTS*]-(b)) WITH collect(p) AS paths, a, b
MATCH (c:Character) WHERE all(x IN paths WHERE c IN nodes(x)) AND NOT c IN [a,b]
RETURN a.name, b.name, c.name AS PivotalNode SKIP 490 LIMIT 10
'''):
    print(r)

for r in graph.run('''
MATCH (a:Character {name: "Drogo"}), (b:Character {name: "Ramsay"})
MATCH p=allShortestPaths((a)-[:INTERACTS*]-(b))
RETURN p
'''):
    print(r)

# ##Centrality measures
# Degree centrality

for r in graph.run('''
MATCH (c:Character)-[:INTERACTS]-()
RETURN c.name AS character, count(*) AS degree ORDER BY degree DESC LIMIT 10
'''):
    print(r)

# Weighted degree centrality
for r in graph.run('''
MATCH (c:Character)-[r:INTERACTS]-()
RETURN c.name AS character, sum(r.weight) AS weightedDegree ORDER BY weightedDegree DESC LIMIT 10
'''):
    print(r)

# Betweenness centrality
# for r in graph.run('''
# MATCH (c:Character)
# WITH collect(c) AS characters
# CALL apoc.algo.betweenness(['INTERACTS'], characters, 'BOTH') YIELD node, score
# SET node.betweenness = score
# RETURN node.name AS name, score ORDER BY score DESC LIMIT 10
# '''):
#     print(r)

# Closeness centrality
# for r in graph.run('''
# MATCH (c:Character)
# WITH collect(c) AS characters
# CALL apoc.algo.closeness(['INTERACTS'], characters, 'BOTH') YIELD node, score
# RETURN node.name AS name, score ORDER BY score DESC LIMIT 10
# '''):
#     print(r)

'''
url:https://www.bilibili.com/video/BV1K8411v771/
url:https://zhuanlan.zhihu.com/p/685892805/
投影到图目录中，以准备进行算法执行。使用一个本机投影来定位Character节点和INTERACTS关系。
  CALL gds.graph.project('myGraph', 'Character', {INTERACTS: {properties: 'weight'}}) 
  CALL gds.graph.project('myUndirectedGraph', 'Character', {INTERACTS: {orientation: 'UNDIRECTED'}}) 无向图
估计运行算法的内存需求：
 CALL gds.betweenness.write.estimate('myGraph', { writeProperty: 'betweenness' })
 YIELD nodeCount, relationshipCount, bytesMin, bytesMax, requiredMemory
以单线程运行算法的内存需求：
 CALL gds.betweenness.write.estimate('myGraph', { writeProperty: 'betweenness', concurrency: 1 })
 YIELD nodeCount, relationshipCount, bytesMin, bytesMax, requiredMemory
以stream模式运行算法：
 CALL gds.betweenness.stream('myGraph')
 YIELD nodeId, score
 RETURN gds.util.asNode(nodeId).name AS name, score
 ORDER BY name ASC
以stats模式运行算法：
 CALL gds.betweenness.stats('myGraph')
 YIELD centralityDistribution
 RETURN centralityDistribution.min AS minimumScore, centralityDistribution.mean AS meanScore
以mutate模式运行算法： 
 CALL gds.betweenness.mutate('myGraph', { mutateProperty: 'betweenness' })
 YIELD centralityDistribution, nodePropertiesWritten
 RETURN centralityDistribution.min AS minimumScore, centralityDistribution.mean AS meanScore, nodePropertiesWritten 
以write模式运行算法：
 CALL gds.betweenness.write('myGraph', { writeProperty: 'betweenness' })
 YIELD centralityDistribution, nodePropertiesWritten
 RETURN centralityDistribution.min AS minimumScore, centralityDistribution.mean AS meanScore, nodePropertiesWritten
以抽样大小为2的stream模式运行算法：
 CALL gds.betweenness.stream('myGraph', {samplingSize: 2, samplingSeed: 0})
 YIELD nodeId, score
 RETURN gds.util.asNode(nodeId).name AS name, score
 ORDER BY name ASC
在无向图上以stream模式运行算法：
 CALL gds.betweenness.stream('myUndirectedGraph')
 YIELD nodeId, score
 RETURN gds.util.asNode(nodeId).name AS name, score
 ORDER BY name ASC
在带权重的图上以stream模式运行算法：
 CALL gds.betweenness.stream('myGraph', {relationshipWeightProperty: 'weight'})
 YIELD nodeId, score
 RETURN gds.util.asNode(nodeId).name AS name, score
 ORDER BY name ASC
'''

# graph.run('''CALL gds.graph.project('myGraph', 'Character', {INTERACTS: {properties: 'weight'}})''')
for r in graph.run('''
CALL gds.betweenness.stream('myGraph')
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).name AS name, score
ORDER BY name ASC LIMIT 10
'''):
    print(r)

for r in graph.run('''
CALL gds.closeness.stream('myGraph')
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).name AS name, score
ORDER BY name ASC LIMIT 10
'''):
    print(r)

# 新版带参
query = "MATCH (n:Character {name: $name}) RETURN n"
result = graph.run(query, parameters={'name':"Daenerys"})
print(result)

# ##Using python-igraph
# Building an igraph instance from Neo4j
from igraph import Graph as IGraph
query = '''
MATCH (c1:Character)-[r:INTERACTS]->(c2:Character)
RETURN c1.name, c2.name, r.weight AS weight
'''
ig = IGraph.TupleList(graph.run(query), weights=True)
print(ig)
# PageRank
pg = ig.pagerank()
pgvs = []
for p in zip(ig.vs, pg):
    pgvs.append({"name": p[0]["name"], "pg": p[1]})
print(pgvs[:5])

write_clusters_query = '''
UNWIND $nodes AS n
MATCH (c:Character) WHERE c.name = n.name
SET c.pagerank = n.pg
'''
graph.run(write_clusters_query, parameters={'nodes':pgvs})

for r in graph.run('''
MATCH (n:Character)
RETURN n.name AS name, n.pagerank AS pagerank ORDER BY pagerank DESC LIMIT 10
'''):
    print(r)

# Community detection

clusters = IGraph.community_walktrap(ig, weights="weight").as_clustering()

nodes = [{"name": node["name"]} for node in ig.vs]
for node in nodes:
    idx = ig.vs.find(name=node["name"]).index
    node["community"] = clusters.membership[idx]

print(nodes[:5])

write_clusters_query = '''
UNWIND $nodes AS n
MATCH (c:Character) WHERE c.name = n.name
SET c.community = toInteger(n.community)
'''
graph.run(write_clusters_query, parameters={'nodes':nodes})

for r in graph.run('''
MATCH (c:Character)
WITH c.community AS cluster, collect(c.name) AS  members
RETURN cluster, members ORDER BY cluster ASC
'''):
    print(r)

# Visualization
# https://github.com/neo4j-contrib/neovis.js
# examples/simple-example.html