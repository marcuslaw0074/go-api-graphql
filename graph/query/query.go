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
	with collect(p) as p, collect(Distinct system) as b
	match (f)-[:subClassOf*]->(r) where f in b 
	Unwind r+b as w
	with collect(DISTINCT w) as w, p+w as x
	Unwind x as qq
	with collect (qq) as x
	match (c)-[:subClassOf]->(s) where c in x and s in x return s.name as o, c.name as s`

QueryAllsys string = `MATCH (system:brick {database:$database, 
	measurement:$measurement}) - 
	[r:subClassOf] -> (p:brick {database:
	$database, measurement:$measurement})
	WHERE exists(({database:$database, 
	measurement:$measurement})-
	[:isPartOf|isPointOf*]->(system)-[:subClassOf*]->
	(:brick {name: 'System', database:$database, 
	measurement:$measurement}))
	with collect(p) as p, collect(Distinct system) as b
	match (f)-[:subClassOf*]->(r) where f in b 
	Unwind r+b as w
	with collect(DISTINCT w) as w, p+w as x
	Unwind x as qq
	with collect (qq) as x
	match (c)-[:subClassOf]->(s) where c in x and s in x return s.name as o, c.name as s`

QueryAlllocbysysEnergy string = `match (b:elecbrick {database:$database, 
	measurement:$measurement})<-[:subClassOf]-(p)
	WHERE exists((:elecbrick{name:$system, 
	database:$database, measurement:$measurement })<-
	[:isLocationOf|isPartOf|isPointOf|feeds*]-(p)-[:subClassOf*]->
	(:elecbrick {name: "Location", database:$database, measurement:$measurement })) 
	with collect(p) as p, collect(Distinct b) as b
	match (f)-[:subClassOf*]->(r) where f in b 
	Unwind r+b as w
	with collect(DISTINCT w) as w, p+w as x
	Unwind x as qq
	with collect (qq) as x
	match (c)-[:subClassOf]->(s) where c in x and s in x return s.name as o,c.name as s`

QueryAlllocbysys string = `match (b:brick {database:$database, 
	measurement:$measurement})<-[:subClassOf]-(p)
	WHERE exists((:brick{name:$system, 
	database:$database, measurement:$measurement })<-
	[:isLocationOf|isPartOf|isPointOf|feeds*]-(p)-[:subClassOf*]->
	(:brick {name: "Location", database:$database, measurement:$measurement })) 
	with collect(p) as p, collect(Distinct b) as b
	match (f)-[:subClassOf*]->(r) where f in b 
	Unwind r+b as w
	with collect(DISTINCT w) as w, p+w as x
	Unwind x as qq
	with collect (qq) as x
	match (c)-[:subClassOf]->(s) where c in x and s in x return s.name as o,c.name as s`


QueryAllequipbysyslocEnergy string = `match (m:elecbldg)-[:isLocationOf]->(p)
	-[:isPointOf]->(j:elecbrick)-[:isPartOf]->(g:elecbrick) 
	where m.name=$location and g.name=$system
	and g.database=$database and g.measurement=$measurement
	return p.name as name`

QueryAllequipbysysloc string = `match (m:bldg)-[:isLocationOf]->(p)
	-[:isPointOf]->(j:brick)-[:isPartOf]->(g:brick) 
	where m.name=$location and g.name=$system
	and g.database=$database and g.measurement=$measurement
	return p.name as name`

QueryAllparambyequipEnergy string = `match (s)-[:hasPoint]->(p)-[:isPartOf]->(m: elecbldg) 
	where m.name=$equips and m.database=$database and 
	m.measurement=$measurement return p.name as label, p.BMS_id as value`

QueryAllparambyequip string = `match (s)-[:hasPoint]->(p)-[:isPartOf]->(m: bldg) 
	where m.name=$equips and m.database=$database and 
	m.measurement=$measurement return p.name as label, p.BMS_id as value`
)