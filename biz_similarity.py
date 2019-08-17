from neo4j import GraphDatabase
import json
from datetime import datetime
import sys

print("Start")

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo"))
print("Start : biz_similarity.py")
now = datetime.now()
print( now )
sys.stdout.flush()

with driver.session() as session:
	session.run('''
CALL apoc.periodic.iterate(
"
MATCH (b1:Business) RETURN b1
",
"
MATCH (b1)-[v1:VISITED]-()-[v2:VISITED]-(b2:Business)
WHERE b1.state = b2.state and b1.id > b2.id
WITH b1,b2,count(*) as coop, collect(v1.stars) as s1, collect(v2.stars) as s2 where coop > 4
WITH b1,b2, coop, algo.similarity.pearson(s1,s2) as similarity WHERE similarity > 0
MERGE (b1)-[s:BUSINESS_SIMILARITY]-(b2) SET s.pearson_similarity = similarity, s.cooccurence = coop
"
, {batchSize:1, parallel:false,iterateList:true});
                               ''')
print("END : biz_similarity.py")
now = datetime.now()
print( now )
sys.stdout.flush()
