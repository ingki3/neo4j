from neo4j import GraphDatabase
import json
from datetime import datetime
import sys

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo"))
print("Start : visited.py")
now = datetime.now()
print( now )
sys.stdout.flush()

with driver.session() as session:
    session.run('''
            CALL apoc.periodic.iterate("
            match (u:User)-[:WROTE]->(r:Review)-[:REVIEWS]->(b:Business)
            return u, b, r
           ","
            merge (u)-[v:VISITED]->(b)
            set v.stars = r.stars, v.user_id =u.id, v.business_id=b.id
",{batchSize: 500, iterateList: true});
                ''')
print("END : visited.py")
now = datetime.now()
print( now )