USE test;

DROP PROCEDURE IF EXISTS `exists_view`;
DROP PROCEDURE IF EXISTS `creatView`;

DELIMITER $$ 

CREATE PROCEDURE exists_view(IN viewName VARCHAR(500), OUT re BOOLEAN)   
 BEGIN   
   DECLARE num INT;
   SELECT COUNT(information_schema.VIEWS.TABLE_SCHEMA) INTO num
	FROM information_schema.VIEWS
	WHERE (information_schema.VIEWS.TABLE_SCHEMA=viewName); 
	
   IF(num != 0 ) THEN
        SET re=TRUE;
   ELSE 
        SET re=FALSE;
   END IF;
	
END $$                                          


CREATE PROCEDURE creatView()
BEGIN
DECLARE isExits1 BOOLEAN;
DECLARE isExits2 BOOLEAN;
DECLARE isExits3 BOOLEAN;
DECLARE isExits4 BOOLEAN;
DECLARE isExits5 BOOLEAN;
DECLARE isExits6 BOOLEAN;

CALL exists_view('insertResultView',isExits1);
SELECT isExits1;
IF(isExits1 = FALSE) THEN
CREATE
    /*[ALGORITHM = {UNDEFINED | MERGE | TEMPTABLE}]
    [DEFINER = { user | CURRENT_USER }]
    [SQL SECURITY { DEFINER | INVOKER }]*/
    VIEW `insertResultView` (projectID,createSchemaTime,totalPoints,totalTimes,totalErrorPoint)
    AS
(SELECT a.projectID,a.result_value,b.result_value,c.result_value,d.result_value FROM (
(SELECT projectID, result_value FROM RESULT WHERE result_key = 'createSchemaTime(s)') a  JOIN 
(SELECT projectID, result_value FROM RESULT WHERE result_key = 'totalPoints') b ON a.projectID = b.projectID JOIN
(SELECT projectID, result_value FROM RESULT WHERE result_key = 'totalTime(s)') c ON a.projectID = c.projectID JOIN
(SELECT projectID, result_value FROM RESULT WHERE result_key = 'totalErrorPoint') d ON a.projectID = d.projectID)
);
END IF;

CALL exists_view('queryResultView',isExits2);
SELECT isExits2;
IF(isExits2 = FALSE) THEN
CREATE
    /*[ALGORITHM = {UNDEFINED | MERGE | TEMPTABLE}]
    [DEFINER = { user | CURRENT_USER }]
    [SQL SECURITY { DEFINER | INVOKER }]*/
    VIEW `queryResultView`(projectID,queryNumber,totalPoints,totalTimes,totalErrorPoint,resultPointPerSecond)
    AS
(SELECT a.projectID,a.result_value,b.result_value,c.result_value,d.result_value,e.result_value FROM 
(SELECT projectID, result_value FROM RESULT WHERE result_key = 'queryNumber') a  JOIN 
(SELECT projectID, result_value FROM RESULT WHERE result_key = 'totalPoint') b ON a.projectID = b.projectID JOIN
(SELECT projectID, result_value FROM RESULT WHERE result_key = 'totalTime(s)') c ON a.projectID = c.projectID JOIN
(SELECT projectID, result_value FROM RESULT WHERE result_key = 'totalErrorQuery') d ON a.projectID = d.projectID JOIN
(SELECT projectID, result_value FROM RESULT WHERE result_key = 'resultPointPerSecond(points/s)') e ON a.projectID = e.projectID
);
END IF;

CALL exists_view('configCommonInfoView',isExits3);
SELECT isExits3;
IF(isExits3 = FALSE) THEN
CREATE
    /*[ALGORITHM = {UNDEFINED | MERGE | TEMPTABLE}]
    [DEFINER = { user | CURRENT_USER }]
    [SQL SECURITY { DEFINER | INVOKER }]*/
    VIEW `configCommonInfoView`(projectID,`mode`,dbSwitch,`version`,clientNumber,`loop`,serverIP,clientName)
    AS
(SELECT a.projectID,a.configuration_value,b.configuration_value,c.configuration_value,d.configuration_value,e.configuration_value ,f.configuration_value,g.configuration_value FROM 
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = 'MODE') a  JOIN 
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = 'DB_SWITCH') b ON a.projectID = b.projectID JOIN
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = 'VERSION') c ON a.projectID = c.projectID JOIN
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = 'CLIENT_NUMBER') d ON a.projectID = d.projectID JOIN
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = 'LOOP') e ON a.projectID = e.projectID JOIN
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = 'ServerIP') f ON a.projectID = f.projectID JOIN
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = 'CLIENT') g ON a.projectID = g.projectID
);
END IF;

CALL exists_view('configInsertInfoView',isExits4);
SELECT isExits4;
IF(isExits4 = FALSE) THEN
CREATE
    /*[ALGORITHM = {UNDEFINED | MERGE | TEMPTABLE}]
    [DEFINER = { user | CURRENT_USER }]
    [SQL SECURITY { DEFINER | INVOKER }]*/
    VIEW `configInsertInfoView`(projectID,`mode`,dbSwitch,`version`,clientNumber,`loop`,serverIP,clientName,
    IS_OVERFLOW,MUL_DEV_BATCH,GROUP_NUMBER,DEVICE_NUMBER,SENSOR_NUMBER,CACHE_NUM,POINT_STEP,ENCODING)
    AS
(SELECT  DISTINCT (a.projectID),a.mode,a.dbSwitch,a.version,a.clientNumber,a.loop,a.serverIP,a.clientName,aa.configuration_value,
b.configuration_value,d.configuration_value,e.configuration_value ,f.configuration_value,g.configuration_value ,h.configuration_value,i.configuration_value FROM 
(SELECT * FROM configCommonInfoView ) a  JOIN
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = 'IS_OVERFLOW') aa  ON a.projectID = aa.projectID JOIN 
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = 'MUL_DEV_BATCH') b ON a.projectID = b.projectID JOIN
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = 'GROUP_NUMBER') d ON a.projectID = d.projectID JOIN
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = 'DEVICE_NUMBER') e ON a.projectID = e.projectID JOIN
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = 'SENSOR_NUMBER') f ON a.projectID = f.projectID JOIN
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = 'CACHE_NUM') g ON a.projectID = g.projectID JOIN
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = 'POINT_STEP') h ON a.projectID = h.projectID LEFT JOIN
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = 'ENCODING') i ON a.projectID = i.projectID
);
END IF;

CALL exists_view('configQueryBasicInfoView',isExits5);
SELECT isExits5;
IF(isExits5 = FALSE) THEN
CREATE
    /*[ALGORITHM = {UNDEFINED | MERGE | TEMPTABLE}]
    [DEFINER = { user | CURRENT_USER }]
    [SQL SECURITY { DEFINER | INVOKER }]*/
    VIEW `configQueryBasicInfoView`(projectID,QUERY_CHOICE,QUERY_DEVICE_NUM,QUERY_SENSOR_NUM,查询数据集存储组数,查询数据集设备数,查询数据集传感器数,IOTDB编码方式)
    AS
(SELECT a.projectID,a.configuration_value,b.configuration_value,c.configuration_value,d.configuration_value,e.configuration_value ,f.configuration_value,g.configuration_value  FROM 
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = 'QUERY_CHOICE') a  JOIN 
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = 'QUERY_DEVICE_NUM') b ON a.projectID = b.projectID JOIN
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = 'QUERY_SENSOR_NUM') c ON a.projectID = c.projectID JOIN
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = '查询数据集存储组数') d ON a.projectID = d.projectID JOIN
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = '查询数据集设备数') e ON a.projectID = e.projectID JOIN
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = '查询数据集传感器数') f ON a.projectID = f.projectID LEFT JOIN
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = 'IOTDB编码方式') g ON a.projectID = g.projectID
);

END IF;

CALL exists_view('configQueryInfoView',isExits6);
SELECT isExits6;
IF(isExits6 = FALSE) THEN
CREATE
    /*[ALGORITHM = {UNDEFINED | MERGE | TEMPTABLE}]
    [DEFINER = { user | CURRENT_USER }]
    [SQL SECURITY { DEFINER | INVOKER }]*/
    VIEW `configQueryInfoView`(projectID,dbSwitch,`version`,QUERY_CHOICE,`LOOP`,CLIENT_NUMBER,QUERY_DEVICE_NUM,QUERY_SENSOR_NUM,IS_RESULTSET_NULL,QUERY_AGGREGATE_FUN,
    TIME_INTERVAL,FILTRATION_CONDITION,TIME_UNIT)
    AS
(SELECT  DISTINCT (a.projectID),aa.dbSwitch,aa.version,a.QUERY_CHOICE, aa.loop, aa.clientNumber,a.QUERY_DEVICE_NUM,a.QUERY_SENSOR_NUM,
b.configuration_value,d.configuration_value,e.configuration_value ,f.configuration_value,g.configuration_value FROM 
(SELECT projectID,QUERY_CHOICE,QUERY_DEVICE_NUM,QUERY_SENSOR_NUM FROM configQueryBasicInfoView) a  JOIN
(SELECT projectID, `loop`,clientNumber,dbSwitch,`version` FROM configCommonInfoView) aa  ON a.projectID = aa.projectID LEFT JOIN 
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = 'IS_RESULTSET_NULL') b ON a.projectID = b.projectID LEFT JOIN
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = 'QUERY_AGGREGATE_FUN') d ON a.projectID = d.projectID LEFT  JOIN
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = 'TIME_INTERVAL') e ON a.projectID = e.projectID LEFT  JOIN
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = 'FILTRATION_CONDITION') f ON a.projectID = f.projectID LEFT  JOIN
(SELECT projectID, configuration_value FROM CONFIG WHERE configuration_item = 'TIME_UNIT') g ON a.projectID = g.projectID
);
END IF;


END $$ 

DELIMITER ;
CALL creatView();
