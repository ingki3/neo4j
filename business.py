from neo4j import GraphDatabase
import json

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo"))
print("Start")
with driver.session() as session:
    session.run('''
                CALL apoc.periodic.iterate("
                CALL apoc.load.json('file:///business.json') YIELD value RETURN value
                ","
                MERGE (b:Business{id:value.business_id})
                SET b += apoc.map.clean(value, ['attributes','hours','business_id','categories','address','postal_code'],[])
                WITH b,value.categories as categories
                UNWIND categories as category
                MERGE (c:Category{id:category})
                MERGE (b)-[:IN_CATEGORY]->(c)
                ",{batchSize: 10000, iterateList: true});
                ''')
print("END")

