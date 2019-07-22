import json

print("Start")
with open("/mnt/disks/ssd/neo4j/import/user.json", 'r') as json_reader:
    with open("/mnt/disks/ssd/neo4j/import/user_trans.json", "w" ) as json_writer:
        for line in file.readlines():
            item = json.loads(line)
            user_id = "\"user_id\":\"" + item["user_id"] + "\""
            friend_list = '\"friends\":[\"' + item["friends"].replace('\"','').replace(',', '\",\"') + '\"]'
            json_item = "{" + user_id + "," + friend_list + "}\n"
            json_writer.write(json_item)
print("END")

