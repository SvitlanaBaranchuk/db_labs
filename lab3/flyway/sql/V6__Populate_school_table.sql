INSERT INTO School (Name, ParentName, Place_ID)
SELECT DISTINCT EOName, EOParent,
(SELECT DISTINCT a.Place_ID
FROM (SELECT DISTINCT Place_ID, RegName, AreaName, TerName
	FROM Place
	WHERE RegName IS NOT NULL AND AreaName IS NOT NULL AND TerName IS NOT NULL) as a
WHERE a.RegName=zno.region AND a.AreaName=zno.area AND
a.TerName=zno.tername) as place_id
FROM zno
ON CONFLICT DO NOTHING;

INSERT INTO School (Name, ParentName, Place_ID)
SELECT DISTINCT ukrPTName, null,
(SELECT DISTINCT Place_ID
FROM (SELECT DISTINCT Place_ID, RegName, AreaName, TerName
	FROM Place
	WHERE RegName IS NOT NULL AND AreaName IS NOT NULL AND TerName IS NOT NULL) as a
WHERE RegName=ukrPTReg AND AreaName=ukrPTArea AND
TerName=ukrPTTer) as place_id
FROM zno
ON CONFLICT DO NOTHING;

INSERT INTO School (Name, ParentName, Place_ID)
SELECT DISTINCT histPTName, null,
(SELECT DISTINCT Place_ID
FROM (SELECT DISTINCT Place_ID, RegName, AreaName, TerName
	FROM Place
	WHERE RegName IS NOT NULL AND AreaName IS NOT NULL AND TerName IS NOT NULL) as a
WHERE RegName=histPTReg AND AreaName=histPTArea AND
TerName=histPTTer) as place_id
FROM zno
ON CONFLICT DO NOTHING;

INSERT INTO School (Name, ParentName, Place_ID)
SELECT DISTINCT mathPTName, null,
(SELECT DISTINCT Place_ID
FROM (SELECT DISTINCT Place_ID, RegName, AreaName, TerName
	FROM Place
	WHERE RegName IS NOT NULL AND AreaName IS NOT NULL AND TerName IS NOT NULL) as a
WHERE RegName=mathPTReg AND AreaName=mathPTArea AND
TerName=mathPTTer) as place_id
FROM zno
ON CONFLICT  DO NOTHING;

INSERT INTO School (Name, ParentName, Place_ID)
SELECT DISTINCT physPTName, null,
(SELECT DISTINCT Place_ID
FROM (SELECT DISTINCT Place_ID, RegName, AreaName, TerName
	FROM Place
	WHERE RegName IS NOT NULL AND AreaName IS NOT NULL AND TerName IS NOT NULL) as a
WHERE RegName=physPTReg AND AreaName=physPTArea AND
TerName=physPTTer) as place_id
FROM zno
ON CONFLICT DO NOTHING;

INSERT INTO School (Name, ParentName, Place_ID)
SELECT DISTINCT chemPTName, null,
(SELECT DISTINCT Place_ID
FROM (SELECT DISTINCT Place_ID, RegName, AreaName, TerName
	FROM Place
	WHERE RegName IS NOT NULL AND AreaName IS NOT NULL AND TerName IS NOT NULL) as a
WHERE RegName=chemPTReg AND AreaName=chemPTArea AND
TerName=chemPTTer) as place_id
FROM zno
ON CONFLICT DO NOTHING;

INSERT INTO School (Name, ParentName, Place_ID)
SELECT DISTINCT bioPTName, null,
(SELECT Place_ID
FROM (SELECT DISTINCT Place_ID, RegName, AreaName, TerName
	FROM Place
	WHERE RegName IS NOT NULL AND AreaName IS NOT NULL AND TerName IS NOT NULL) as a
WHERE RegName=bioPTReg AND AreaName=bioPTArea AND
TerName=bioPTTer) as place_id
FROM zno
ON CONFLICT DO NOTHING;

INSERT INTO School (Name, ParentName, Place_ID)
SELECT DISTINCT geoPTName, null,
(SELECT DISTINCT Place_ID
FROM (SELECT DISTINCT Place_ID, RegName, AreaName, TerName
	FROM Place
	WHERE RegName IS NOT NULL AND AreaName IS NOT NULL AND TerName IS NOT NULL) as a
WHERE RegName=geoPTReg AND AreaName=geoPTArea AND
TerName=geoPTTer) as place_id
FROM zno
ON CONFLICT DO NOTHING;

INSERT INTO School (Name, ParentName, Place_ID)
SELECT DISTINCT engPTName, null,
(SELECT DISTINCT Place_ID
FROM (SELECT DISTINCT Place_ID, RegName, AreaName, TerName
	FROM Place
	WHERE RegName IS NOT NULL AND AreaName IS NOT NULL AND TerName IS NOT NULL)  as a
WHERE RegName=engPTReg AND AreaName=engPTArea AND
TerName=engPTTer) as place_id
FROM zno
ON CONFLICT DO NOTHING;

INSERT INTO School (Name, ParentName, Place_ID)
SELECT DISTINCT fraPTName, null,
(SELECT DISTINCT Place_ID
FROM (SELECT DISTINCT Place_ID, RegName, AreaName, TerName
	FROM Place
	WHERE RegName IS NOT NULL AND AreaName IS NOT NULL AND TerName IS NOT NULL) as a
WHERE RegName=fraPTReg AND AreaName=fraPTArea AND
TerName=fraPTTer) as place_id
FROM zno
ON CONFLICT DO NOTHING;

INSERT INTO School (Name, ParentName, Place_ID)
SELECT DISTINCT deuPTName, null,
(SELECT DISTINCT Place_ID
FROM (SELECT DISTINCT Place_ID, RegName, AreaName, TerName
	FROM Place
	WHERE RegName IS NOT NULL AND AreaName IS NOT NULL AND TerName IS NOT NULL) as a
WHERE RegName=deuPTReg AND AreaName=deuPTArea AND
TerName=deuPTTer) as place_id
FROM zno
ON CONFLICT DO NOTHING;

INSERT INTO School (Name, ParentName, Place_ID)
SELECT DISTINCT spaPTName, null,
(SELECT DISTINCT Place_ID
FROM (SELECT DISTINCT Place_ID, RegName, AreaName, TerName
	FROM Place
	WHERE RegName IS NOT NULL AND AreaName IS NOT NULL AND TerName IS NOT NULL) as a
WHERE RegName=spaPTReg AND AreaName=spaPTArea AND
TerName=spaPTTer) as place_id
FROM zno
ON CONFLICT DO NOTHING;
