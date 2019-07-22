from neo4j import GraphDatabase
import json

print("Start")
with open("/mnt/disks/ssd/neo4j/import/user.json", 'r') as json_reader:
    with open("/mnt/disks/ssd/neo4j/import/user_trans.json", "w" ) as json_writer:
        for line in file.readlines():
            item = json.loads(line)
            friend_list = '[\"' + item["friends"].replace('\"','').replace(',', '\",\"') + '\"]\n'

print("END")
