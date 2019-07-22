from neo4j import GraphDatabase
import json

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo"))
print("Start")
with driver.session() as session:
    session.run('''
            CALL apoc.periodic.iterate("
            CALL apoc.load.json('file:///review.json')
            YIELD value RETURN value
            ","
            MERGE (b:Business{id:value.business_id})
            MERGE (u:User{id:value.user_id})
            MERGE (r:Review{id:value.review_id})
            MERGE (u)-[:WROTE]->(r)
            MERGE (r)-[:REVIEWS]->(b)
            SET r += apoc.map.clean(value, ['business_id','user_id','review_id','text'],[0])
",{batchSize: 1000, iterateList: true});
                ''')
print("END")
