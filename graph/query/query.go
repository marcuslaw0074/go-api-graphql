package query

const (
QueryAllsysEnergy string = `MATCH (system:elecbrick {database:$database, 
	measurement:$measurement}) - 
	[r:subClassOf] -> (p:elecbrick {database:
	$database, measurement:$measurement})
	WHERE exists(({database:$database, 
	measurement:$measurement})-
	[:isPartOf|isPointOf*]->(system)-[:subClassOf*]->
	(:elecbrick {name: 'System', database:$database, 
	measurement:$measurement}))
	WITH collect(p) AS p, collect(Distinct system) AS b
	MATCH (f)-[:subClassOf*]->(r) WHERE f in b 
	UNWIND r+b AS w
	WITH collect(DISTINCT w) AS w, p+w AS x
	UNWIND x AS qq
	WITH collect (qq) AS x
	MATCH (c)-[:subClassOf]->(s) WHERE c in x AND s in x RETURN s.name AS o, c.name AS s`

QueryAllsys string = `MATCH (system:brick {database:$database, 
	measurement:$measurement}) - 
	[r:subClassOf] -> (p:brick {database:
	$database, measurement:$measurement})
	WHERE exists(({database:$database, 
	measurement:$measurement})-
	[:isPartOf|isPointOf*]->(system)-[:subClassOf*]->
	(:brick {name: 'System', database:$database, 
	measurement:$measurement}))
	WITH collect(p) AS p, collect(Distinct system) AS b
	MATCH (f)-[:subClassOf*]->(r) WHERE f in b 
	UNWIND r+b AS w
	WITH collect(DISTINCT w) AS w, p+w AS x
	UNWIND x AS qq
	WITH collect (qq) AS x
	MATCH (c)-[:subClassOf]->(s) WHERE c in x AND s in x RETURN s.name AS o, c.name AS s`

QueryAlllocbysysEnergy string = `MATCH (b:elecbrick {database:$database, 
	measurement:$measurement})<-[:subClassOf]-(p)
	WHERE exists((:elecbrick{name:$system, 
	database:$database, measurement:$measurement })<-
	[:isLocationOf|isPartOf|isPointOf|feeds*]-(p)-[:subClassOf*]->
	(:elecbrick {name: "Location", database:$database, measurement:$measurement })) 
	WITH collect(p) AS p, collect(Distinct b) AS b
	MATCH (f)-[:subClassOf*]->(r) WHERE f in b 
	UNWIND r+b AS w
	WITH collect(DISTINCT w) AS w, p+w AS x
	UNWIND x AS qq
	WITH collect (qq) AS x
	MATCH (c)-[:subClassOf]->(s) WHERE c in x AND s in x RETURN s.name AS o,c.name AS s`

QueryAlllocbysys string = `MATCH (b:brick {database:$database, 
	measurement:$measurement})<-[:subClassOf]-(p)
	WHERE exists((:brick{name:$system, 
	database:$database, measurement:$measurement })<-
	[:isLocationOf|isPartOf|isPointOf|feeds*]-(p)-[:subClassOf*]->
	(:brick {name: "Location", database:$database, measurement:$measurement })) 
	WITH collect(p) AS p, collect(Distinct b) AS b
	MATCH (f)-[:subClassOf*]->(r) WHERE f in b 
	UNWIND r+b AS w
	WITH collect(DISTINCT w) AS w, p+w AS x
	UNWIND x AS qq
	WITH collect (qq) AS x
	MATCH (c)-[:subClassOf]->(s) WHERE c in x AND s in x RETURN s.name AS o,c.name AS s`

QueryAllequipbysyslocEnergy string = `MATCH (m:elecbldg)-[:isLocationOf]->(p)
	-[:isPointOf]->(j:elecbrick)-[:isPartOf]->(g:elecbrick) 
	WHERE m.name=$location AND g.name=$system
	AND g.database=$database AND g.measurement=$measurement
	RETURN p.name AS name`

QueryAllequipbysysloc string = `MATCH (m:bldg)-[:isLocationOf]->(p)
	-[:isPointOf]->(j:brick)-[:isPartOf]->(g:brick) 
	WHERE m.name=$location AND g.name=$system
	AND g.database=$database AND g.measurement=$measurement
	RETURN p.name AS name`

QueryAllparambyequipEnergy string = `MATCH (s)-[:hasPoint]->(p)-[:isPartOf]->(m: elecbldg) 
	WHERE m.name=$equips AND m.database=$database AND 
	m.measurement=$measurement RETURN p.name AS label, p.BMS_id AS value`

QueryAllparambyequip string = `MATCH (s)-[:hasPoint]->(p)-[:isPartOf]->(m: bldg) 
	WHERE m.name=$equips AND m.database=$database AND 
	m.measurement=$measurement RETURN p.name AS label, p.BMS_id AS value`

QueryAllClientPoint string = `MATCH (n) WHERE n.database=$database AND 
	n.measurement=$measurement AND NOT n.BMS_id IS NULL 
	WITH DISTINCT n.name AS label, n.BMS_id AS value
	RETURN label, value`

QueryAllClientPointBySystem string = `MATCH (n)-[:subClassOf|isPartOf|isPointOf*]->(p)
	WHERE n.database=$database AND n.measurement=$measurement AND NOT n.BMS_id IS NULL 
	AND (p: brick or p: elecbrick) AND p.name=$system
	WITH DISTINCT n.name AS label, n.BMS_id AS value
	RETURN label, value`

QueryAllClientPointBySystemLocation string = `MATCH (m)<-[:isPartOf|isPointOf|hasLocation*]-(n)-
	[:subClassOf|isPartOf|isPointOf*]->(p)
	WHERE n.database=$database AND n.measurement=$measurement AND NOT n.BMS_id IS NULL 
	AND (p: brick or p: elecbrick) AND p.name=$system AND m.name=$location
	WITH DISTINCT n.name AS label, n.BMS_id AS value
	RETURN label, value`
)