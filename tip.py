from neo4j import GraphDatabase
import json
from datetime import datetime

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo"))
print("Start")

now = datetime.now()
print( now )

with driver.session() as session:
    session.run('''
                CALL apoc.periodic.iterate("
                CALL apoc.load.json('file:///tip.json') YIELD value RETURN value
                ","
                MATCH (b:Business{id:value.business_id})
                MERGE (u:User{id:value.user_id})
                MERGE (u)-[:TIP{date:value.date,compliment_count:value.compliment_count,text:value.text}]->(b)
                ",{batchSize: 20000, iterateList: true});
                ''')
print("END")
now = datetime.now()
print( now )
