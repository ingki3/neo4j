from neo4j import GraphDatabase
import json

print("Start")

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo"))
with driver.session() as session:
	session.run('''
CALL apoc.periodic.iterate(
"MATCH (p1:ActiveUser) RETURN p1",
"
MATCH (p1)-[:WROTE]->(r1)-->()<--(r2)<-[:WROTE]-(p2:ActiveUser)
WHERE id(p1) < id(p2)
WITH p1,p2,count(*) as coop, collect(r1.stars) as s1, collect(r2.stars) as s2 where coop > 10
WITH p1,p2, apoc.algo.cosineSimilarity(s1,s2) as cosineSimilarity WHERE cosineSimilarity > 0
MERGE (p1)-[s:SIMILAR_REVIEWS]-(p2) SET s.weight = cosineSimilarity"
, {batchSize:100, parallel:false,iterateList:true});
                               ''')
print("END")