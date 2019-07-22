from neo4j import GraphDatabase
import json

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo"))
print("Start")
with driver.session() as session:
    session.run('''
                CALL apoc.periodic.iterate("
                CALL apoc.load.json('file:///user.json')
                YIELD value RETURN value
                ","
                MERGE (u:User{id:value.user_id})
                SET u += apoc.map.clean(value, ['friends','user_id'],[0])
                ",{batchSize: 100, iterateList: true});
                ''')
print("END")
