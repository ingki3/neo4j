from neo4j import GraphDatabase
import json

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo"))
print("Start")
with driver.session() as session:
    session.run('''
                MATCH (p1:User) WHERE p1.review_count > 5
                SET p1:ActiveUser
                RETURN p1
                ''')
print("END")

