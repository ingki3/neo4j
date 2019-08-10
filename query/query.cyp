match (b1:Business{id:"QXAEGFB4oINsVuTFxEYKFQ"})<-[:REVIEWS]-(r)
match (r)<-[:WROTE]-(u)
match (u)-[:WROTE]->(r2)
match (r2)-[:REVIEWS]->(b2)
where id(b1) < id(b2) and b2.city = "Mississauga"
With b1, b2, count(*) as weight
Return b2.name, weight
Order by weight DESC
limit 10

CALL apoc.periodic.iterate(
"
match (b1:Business{id: 'QXAEGFB4oINsVuTFxEYKFQ'})<-[:REVIEWS]-(r)
match (r)<-[:WROTE]-(p1)
Return p1
",
"
MATCH (p1)-[:WROTE]->(r1)-->()<--(r2)<-[:WROTE]-(p2)
WHERE id(p1) < id(p2) AND size((p2)-[:WROTE]->()) > 10
WITH p1,p2,count(*) as coop, collect(r1.stars) as s1, collect(r2.stars) as s2 where coop > 10
WITH p1,p2, apoc.algo.cosineSimilarity(s1,s2) as cosineSimilarity WHERE cosineSimilarity > 0
MERGE (p1)-[s:SIMILAR_REVIEWS]-(p2) SET s.weight = cosineSimilarity
Match [link:SIMILAR_REVIEWS]
Return link
"
, {batchSize:100, parallel:false,iterateList:true});


MATCH (b1:Business{id: 'QXAEGFB4oINsVuTFxEYKFQ'})<-[:REVIEWS]-(r)
MATCH (r1)<-[:WROTE]-(p1)
MATCH (p1)-[:WROTE]->(r2)-->(b2)
WHERE id(b1) < id(b2)
WITH b1,b2,count(*) as coop, collect(r1.stars) as s1, collect(r2.stars) as s2
WITH b1,b2, apoc.algo.cosineSimilarity(s1,s2) as cosineSimilarity WHERE cosineSimilarity > 0
MERGE (b1)-[s:BIZ_SIMILAR_REVIEWS{weight:cosineSimilarity}]->(b2)
WITH b1
MATCH (b1)-[link:BIZ_SIMILAR_REVIEWS]->()
RETURN link.weight


match (b:Business{id: 'QXAEGFB4oINsVuTFxEYKFQ'})<-[:REVIEWS]-(r)<-[:WROTE]-(p:User)-[:FRIEND]-(p1:User)-[:WROTE]->(r1)
Match (b1)<-[link:REVIEWS]-(r1)
Return b1.id, b1.city, count(link) as weight
order by weight desc
limit 10





match (b:Business{id: 'QXAEGFB4oINsVuTFxEYKFQ'})<-[:REVIEWS]-(r)<-[:WROTE]-(p:User)-[:FRIEND]-(p1:User)-[:WROTE]->(r1)
Match (b1)<-[link:REVIEWS]-(r1)
With b.city as origin, b1.id as id, b1.name as name, b1.city as city, count(link) as weight, b1 as b1, b as b
Where weight > 10 AND origin <> city
Return id, city, name, weight, toInteger(distance(point({ latitude:b.latitude, longitude:b.longitude }), point({ latitude:b1.latitude, longitude:b1.longitude }))/1000) AS km
Order by weight desc
limit 10


match (b:Business{id:'k6zmSLmYAquCpJGKNnTgSQ'})<-[:REVIEWS]-(r:Review)<-[:WROTE]-(p:User)-[:FRIEND]-(p1:User)-[:WROTE]->(r1)
Match (b1)<-[:REVIEWS]-(r1)
With b.city as origin, b1.id as id, b1.name as name, b1.city as city, count(r) as weight, b1 as b1, b as b, r.stars as stars, r as r, r1 as r1
Where weight > 10 AND origin <> city AND stars > 3
Return id, city, name, count(r) as weight, toInteger(distance(point({ latitude:b.latitude, longitude:b.longitude }), point({ latitude:b1.latitude, longitude:b1.longitude }))/1000) as km
Order by weight desc
limit 10

match (b:Business{id:'k6zmSLmYAquCpJGKNnTgSQ'})<-[:REVIEWS]-(r:Review)<-[:WROTE]-(p:User)-[:FRIEND]-(p1:User)-[:WROTE]->(r1)
Match (b1)<-[:REVIEWS]-(r1)
With b.city as origin, b1.id as id, b1.name as name, b1.city as city, count(r) as weight, b1 as b1, b as b, r as r
Where weight > 10 AND origin <> city
Return id, city, name, count(r) as weight, toInteger(distance(point({ latitude:b.latitude, longitude:b.longitude }), point({ latitude:b1.latitude, longitude:b1.longitude }))/1000) as km
Order by weight desc
limit 10

match (b:Business{id:'QXAEGFB4oINsVuTFxEYKFQ'})<-[:REVIEWS]-(r:Review)<-[:WROTE]-(p:User)-[:FRIEND]-(p1:User)-[:WROTE]->(r1)
Match (b1)<-[:REVIEWS]-(r1)
With b.city as origin, b1.id as id, b1.name as name, b1.city as city, count(r) as weight, b1 as b1, b as b, r as r
Where weight > 10 AND origin <> city
Return id, city, name, count(r) as weight, toInteger(distance(point({ latitude:b.latitude, longitude:b.longitude }), point({ latitude:b1.latitude, longitude:b1.longitude }))/1000) as km
Order by weight desc
limit 10

match (b:Business{id:'k6zmSLmYAquCpJGKNnTgSQ'})<-[:REVIEWS]-(r:Review)<-[:WROTE]-(p:User)-[:FRIEND]-(p1:User)-[:WROTE]->(r1)
Match (b1)<-[:REVIEWS]-(r1)
WHERE r.stars > 3 AND r1.stars >3
return count(b1)

match (b:Business{id:'k6zmSLmYAquCpJGKNnTgSQ'})<-[:REVIEWS]-(r:Review)<-[:WROTE]-(p:User)-[:WROTE]->(r1)
Match (b1)<-[:REVIEWS]-(r1)
return count(b1)

match (b:Business{id:'k6zmSLmYAquCpJGKNnTgSQ'})<-[:REVIEWS]-(r:Review)<-[:WROTE]-(p:User)-[:WROTE]->(r1)
Match (b1)<-[:REVIEWS]-(r1)
With b.city as origin, b1.id as id, b1.name as name, b1.city as city, count(r) as weight, b1 as b1, b as b, r.stars as stars, r as r, r1 as r1
Return id, city, name, count(r) as weight, toInteger(distance(point({ latitude:b.latitude, longitude:b.longitude }), point({ latitude:b1.latitude, longitude:b1.longitude }))/1000) as km
Order by weight desc
limit 10

match (p:User)-[:WROTE]->(r:Review{id:'Q1sbwvVQXV2734tPgoKj4Q'})-[:REVIEWS]->(b:Business)
return p.id, b.id, r.stars
