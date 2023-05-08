INSERT INTO Place(RegName, AreaName, TerName)
SELECT DISTINCT region, area, tername
FROM zno
ON CONFLICT DO NOTHING;

INSERT INTO Place(RegName, AreaName, TerName)
SELECT DISTINCT EOReg, EOArea, EOTer
FROM zno
ON CONFLICT DO NOTHING;

INSERT INTO Place(RegName, AreaName, TerName)
SELECT DISTINCT ukrPTReg, ukrPTArea, ukrPTTer
FROM zno
ON CONFLICT DO NOTHING;

INSERT INTO Place(RegName, AreaName, TerName)
SELECT DISTINCT histPTReg, histPTArea, histPTTer
FROM zno
ON CONFLICT DO NOTHING;

INSERT INTO Place(RegName, AreaName, TerName)
SELECT DISTINCT mathPTReg, mathPTArea, mathPTTer
FROM zno
ON CONFLICT DO NOTHING;

INSERT INTO Place(RegName, AreaName, TerName)
SELECT DISTINCT physPTReg, physPTArea, physPTTer
FROM zno
ON CONFLICT DO NOTHING;

INSERT INTO Place(RegName, AreaName, TerName)
SELECT DISTINCT chemPTReg, chemPTArea, chemPTTer
FROM zno
ON CONFLICT DO NOTHING;

INSERT INTO Place(RegName, AreaName, TerName)
SELECT DISTINCT bioPTReg, bioPTArea, bioPTTer
FROM zno
ON CONFLICT DO NOTHING;

INSERT INTO Place(RegName, AreaName, TerName)
SELECT DISTINCT geoPTReg, geoPTArea, geoPTTer
FROM zno
ON CONFLICT DO NOTHING;

INSERT INTO Place(RegName, AreaName, TerName)
SELECT DISTINCT engPTReg, engPTArea, engPTTer
FROM zno
ON CONFLICT DO NOTHING;

INSERT INTO Place(RegName, AreaName, TerName)
SELECT DISTINCT fraPTReg, fraPTArea, fraPTTer
FROM zno
ON CONFLICT DO NOTHING;

INSERT INTO Place(RegName, AreaName, TerName)
SELECT DISTINCT deuPTReg, deuPTArea, deuPTTer
FROM zno
ON CONFLICT DO NOTHING;

INSERT INTO Place(RegName, AreaName, TerName)
SELECT DISTINCT spaPTReg, spaPTArea, spaPTTer
FROM zno
ON CONFLICT DO NOTHING;