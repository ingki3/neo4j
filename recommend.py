from neo4j import GraphDatabase
import json
from datetime import datetime
import sys

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo"))
print("Start : recommend.py")
now = datetime.now()
print( now )
sys.stdout.flush()

with driver.session() as session:
    session.run('''
            CALL apoc.periodic.iterate("
            MATCH (b1:Business)-[v1:VISITED]-()
            return b1, avg(v1.stars) as avg_stars, count(v1) as v
            ","
            match (b1:Business) where avg_stars > 3.7 and v > 3
            set b1:Recommended
            return b1
            ",{batchSize: 500, iterateList: true});
                ''')
print("END : recommend.py")
now = datetime.now()
print( now )