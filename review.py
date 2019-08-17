from neo4j import GraphDatabase
import json
from datetime import datetime
import sys

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo"))
print("Start : review.py")
now = datetime.now()
print( now )
sys.stdout.flush()

with driver.session() as session:
    session.run('''
            CALL apoc.periodic.iterate("
            CALL apoc.load.json('file:///review.json')
            YIELD value RETURN value
            ","
            match (u:User{id:value.user_id})-[:WROTE]->(r:Review)-[:REVIEWS]->(b:Business{id:value.business_id})
            set r.business_id = value.business_id, r.user_id = value.user_id, r.review_id = value.review_id, r.text = value.text
            return r
",{batchSize: 500, iterateList: true});
                ''')
print("END : review.py")
now = datetime.now()
print( now )
sys.stdout.flush()
