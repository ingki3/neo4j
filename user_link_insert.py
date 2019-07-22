from neo4j import GraphDatabase
import json

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo"))
print("Start")
count = 0
with open("/mnt/disks/ssd/neo4j/import/user_trans.json", 'r') as file:
    for line in file.readlines():
        with driver.session() as session:
            count = count + 1
            item = json.loads(line)
            session.run('''
                        WITH {value} AS value
                        MERGE (u:User{id:value.user_id})
                        WITH u,value.friends as friends
                        UNWIND friends as friend
                        MERGE (u1:User{id:friend})
                        MERGE (u)-[:FRIEND]-(u1)
                        ''', parameters={'value': item}).consume()
        if count % 1000 == 0:
            print("Count : " + str(count))
print("END")
