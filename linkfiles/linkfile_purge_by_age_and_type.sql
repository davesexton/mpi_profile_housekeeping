/**************************************
  Declare config variables
***************************************/
IF VAREXISTS('var_text') != 0 THEN
  DROP VARIABLE var_text;
END IF;
CREATE VARIABLE var_text VARCHAR(8000);

/**************************************
  Set config variables
***************************************/
SET var_text = '
log_file_path=S:\P7UKMPDEV
choke_limit=2
create_months_older_than=60
update_months_older_than=24
type_exclude_list=WCRH,WPCV,WCON,WINT,YQAL
';

/**************************************
 Declare script variables
***************************************/
IF VAREXISTS('log_file_path') != 0 THEN
  DROP VARIABLE log_file_path;
END IF;
IF VAREXISTS('choke_limit') != 0 THEN
  DROP VARIABLE choke_limit;
END IF;
IF VAREXISTS('create_months_older_than') != 0 THEN
  DROP VARIABLE create_months_older_than;
END IF;
IF VAREXISTS('update_months_older_than') != 0 THEN
  DROP VARIABLE update_months_older_than;
END IF;
IF VAREXISTS('type_exclude_list') != 0 THEN
  DROP VARIABLE type_exclude_list;
END IF;
IF VAREXISTS('xtimestamp') != 0 THEN
  DROP VARIABLE xtimestamp;
END IF;
IF VAREXISTS('sql') != 0 THEN
  DROP VARIABLE sql;
END IF;
IF VAREXISTS('return_code') != 0 THEN
  DROP VARIABLE return_code;
END IF;
IF VAREXISTS('csv_file_path') != 0 THEN
  DROP VARIABLE csv_file_path;
END IF;
IF VAREXISTS('rec_count') != 0 THEN
  DROP VARIABLE rec_count;
END IF;
IF VAREXISTS('log_cmd') != 0 THEN
  DROP VARIABLE log_cmd;
END IF;
CREATE VARIABLE log_file_path VARCHAR(900);
CREATE VARIABLE choke_limit INT;
CREATE VARIABLE create_months_older_than INT;
CREATE VARIABLE update_months_older_than INT;
CREATE VARIABLE type_exclude_list VARCHAR(900);
CREATE VARIABLE xtimestamp DATETIME;
CREATE VARIABLE sql VARCHAR(4000);
CREATE VARIABLE return_code INT;
CREATE VARIABLE csv_file_path VARCHAR(900);
CREATE VARIABLE rec_count INT;
CREATE VARIABLE log_cmd VARCHAR(1000);

CREATE TABLE #temp_xlinkfile
(
  xprofile_ref INT
  ,parent_object_ref INT
  ,file_name CHAR(256)
  ,displayname CHAR(64)
  ,type CHAR(4)
  ,record_status CHAR(4)
  ,parent_displayname CHAR(64)
  ,create_timestamp DATETIME
  ,update_timestamp DATETIME
  ,xmode CHAR(1)
  ,xstatus INT
  ,xupdate_timestamp TIMESTAMP
  ,parent_object_name VARCHAR(255)
  ,xerror_value INT
  ,xerror_message VARCHAR(255)
);

--Extract vars from var_text
SET var_text = REPLACE(var_text, '=', ' = ''' );
SET var_text = REPLACE(var_text, CHAR(10), ''';' + CHAR(10) + 'SET ' );
SET var_text = SUBSTR(var_text, 4, LENGTH(var_text) - 7);
EXECUTE(var_text);

-- Make sure choke limit is not above max
IF choke_limit > 32767 THEN
  SET choke_limit = 32767;
END IF;

-- Create timestamp, log file name and CSV name
SET xtimestamp = CAST(LEFT(CAST(GETDATE() AS VARCHAR), 16) AS DATETIME);
IF SUBSTR(log_file_path, LENGTH(log_file_path), 1) = CHAR(92) THEN
  SET log_file_path = SUBSTR(log_file_path, 1, LENGTH(log_file_path) - 1);
END IF;
SET csv_file_path = log_file_path + '\housekeeping_linkfile_purge_by_age_and_type'
   || REPLACE(REPLACE(REPLACE(LEFT(CAST(xtimestamp AS VARCHAR), 16), '-', ''), ':', ''), ' ', '')
   || '.csv';
SET log_file_path = log_file_path + '\housekeeping_linkfile_purge_by_age_and_type'
   || REPLACE(REPLACE(REPLACE(LEFT(CAST(xtimestamp AS VARCHAR), 16), '-', ''), ':', ''), ' ', '')
   || '.log';

-- Start log file
EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': Starting Linkfile Purge by Age and Type >> "' || log_file_path || '"''');

-- Log variables
EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': Variable CHOKE_LIMIT =^> '|| CONVERT(VARCHAR(12), choke_limit) || ' >> "' || log_file_path || '"''');
EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': Variable CREATE_MONTH_OLDER_THAN =^> '|| CONVERT(VARCHAR(12), create_months_older_than) || ' >> "' || log_file_path || '"''');
EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': Variable UPDATE_MONTH_OLDER_THAN =^> '|| CONVERT(VARCHAR(12), update_months_older_than) || ' >> "' || log_file_path || '"''');
EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': Variable TYPE_EXCULUDE_LIST =^> '|| type_exclude_list || ' >> "' || log_file_path || '"''');
EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': Variable LOG_FILE_PATH =^> '|| log_file_path || ' >> "' || log_file_path || '"''');
EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': Variable CSV_FILE_PATH =^> '|| csv_file_path || ' >> "' || log_file_path || '"''');

-- Convert exclude list to a list
SET type_exclude_list = '''' + REPLACE(type_exclude_list, ',', ''',''') + '''';

-- Create sql string to get purge items for PERSON
SET sql = '
INSERT INTO #temp_xlinkfile
(xprofile_ref, parent_object_ref, file_name, displayname, type, record_status, PARENT_DISPLAYNAME, create_timestamp,
 update_timestamp, xmode, xstatus, xupdate_timestamp, parent_object_name, xerror_value, xerror_message)
SELECT TOP ' || CONVERT(VARCHAR(8), choke_limit) || '
  l.linkfile_ref AS xprofile_ref
  ,l.parent_object_ref
  ,l.file_name
  ,REPLACE(l.displayname, ''"'', ''""'')
  ,l.type
  ,l.record_status
  ,p.displayname AS parent_displayname
  ,l.create_timestamp
  ,l.update_timestamp
  ,''D'' AS xmode
  ,20 AS xstatus
  ,CAST( ''' || CAST(xtimestamp AS VARCHAR(16)) || ''' AS DATETIME) AS xupdate_timestamp
  ,l.parent_object_name
  ,CAST(NULL AS INT) AS xerror_value
  ,CAST(NULL AS VARCHAR(255)) AS xerror_message
FROM linkfile l
  LEFT OUTER JOIN person p ON l.parent_object_ref = p.person_ref
WHERE l.parent_object_name IN(''person'')
  AND l.create_timestamp < DATEADD(month,' || CAST(-1 * create_months_older_than AS VARCHAR) || ', GETDATE())
  AND l.update_timestamp < DATEADD(month,' || CAST(-1 * update_months_older_than AS VARCHAR) || ', GETDATE())
  AND l.type NOT IN(' || type_exclude_list || ')
  AND l.update_timestamp < DATEADD(month, -12, GETDATE())
ORDER BY l.update_timestamp
';

EXECUTE (sql);

-- Get count of records to process
SELECT COUNT(*) INTO rec_count FROM #temp_xlinkfile
WHERE parent_object_name = 'person';

-- Check record is more than zero
IF rec_count != 0 THEN

-- Check XWEB is running
  return_code = CALL xinit('xlinkfile_person');
  IF return_code = 0 THEN

-- Add log entry
    SET log_cmd = 'Processing ' || CONVERT(VARCHAR(12), rec_count) || ' xlinkfile_person records';
    EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': ' || log_cmd || ' >> "' || log_file_path || '"''');

-- Load xlinfile_person table
    INSERT INTO xlinkfile_person
    (xprofile_ref, xmode, xstatus, xupdate_timestamp)
    SELECT
      xprofile_ref, xmode, xstatus, xupdate_timestamp
    FROM #temp_xlinkfile
    WHERE parent_object_name = 'person';

-- Run XWEB
    CALL xput('xlinkfile_person', NULL, NULL, NULL, 'Y');

-- Capture results
    UPDATE #temp_xlinkfile
    SET
      x.xstatus = xlf.xstatus
      ,x.xerror_value = xlf.xerror_value
      ,x.xerror_message = xlf.xerror_message
    FROM #temp_xlinkfile x
      INNER JOIN xlinkfile_person xlf ON x.xprofile_ref = xlf.xprofile_ref
    WHERE xlf.xmode = 'D'
      AND xlf.xupdate_timestamp = CAST(xtimestamp AS VARCHAR);

-- Log results
    SELECT ('Procssed xlinkfile_person ' ||
      CAST(SUM(CASE WHEN xstatus >= 40 AND xerror_value = 0
                    THEN 1 ELSE 0 END) AS VARCHAR) || ' passed, ' ||
      CAST(SUM(CASE WHEN xstatus >= 40 AND xerror_value != 0
                    THEN 1 ELSE 0 END) AS VARCHAR) || ' failed, ' ||
      CAST(SUM(CASE WHEN xstatus < 40 THEN 1 ELSE 0 END) AS VARCHAR) || ' unprocessed') INTO log_cmd
    FROM #temp_xlinkfile
    WHERE parent_object_name = 'person';

    EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': ' || log_cmd || ' >> "' || log_file_path || '"''');

  ELSE
    SET log_cmd = 'ERROR: DX not running for xlinkfile_person';
    EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': ' || log_cmd || ' >> "' || log_file_path || '"''');
  END IF;
ELSE
  SET log_cmd = 'No records to process for xlinkfile_person';
  EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': ' || log_cmd || ' >> "' || log_file_path || '"''');
END IF;

GO

-- Create sql string to get purge items for ORGANISATION
EXECUTE (REPLACE(sql, 'person', 'organisation'));

-- Get count of records to process
SELECT COUNT(*) INTO rec_count FROM #temp_xlinkfile
WHERE parent_object_name = 'organisation';

-- Check record is more than zero
IF rec_count != 0 THEN

-- Check XWEB is running
  return_code = CALL xinit('xlinkfile_organisation');
  IF return_code = 0 THEN

-- Add log entry
    SET log_cmd = 'Processing ' || CONVERT(VARCHAR(12), rec_count) || ' xlinkfile_organisation records';
    EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': ' || log_cmd || ' >> "' || log_file_path || '"''');

-- Load xlinfile_organiation table
    INSERT INTO xlinkfile_organisation
    (xprofile_ref, xmode, xstatus, xupdate_timestamp)
    SELECT
      xprofile_ref, xmode, xstatus, xupdate_timestamp
    FROM #temp_xlinkfile
    WHERE parent_object_name = 'organisation';

-- Run XWEB
    CALL xput('xlinkfile_organisation', NULL, NULL, NULL, 'Y');

-- Capture results
    UPDATE #temp_xlinkfile
    SET
      x.xstatus = xlf.xstatus
      ,x.xerror_value = xlf.xerror_value
      ,x.xerror_message = xlf.xerror_message
    FROM #temp_xlinkfile x
      INNER JOIN xlinkfile_organisation xlf ON x.xprofile_ref = xlf.xprofile_ref
    WHERE xlf.xmode = 'D'
      AND xlf.xupdate_timestamp = CAST(xtimestamp AS VARCHAR);

-- Log results
    SELECT ('Procssed xlinkfile_organisation ' ||
      CAST(SUM(CASE xstatus WHEN 40 THEN 1 ELSE 0 END) AS VARCHAR) || ' passed, ' ||
      CAST(SUM(CASE WHEN xstatus > 40 THEN 1 ELSE 0 END) AS VARCHAR) || ' failed, ' ||
      CAST(SUM(CASE WHEN xstatus < 40 THEN 1 ELSE 0 END) AS VARCHAR) || ' unprocessed') INTO log_cmd
    FROM #temp_xlinkfile
    WHERE parent_object_name = 'organisation';

    EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': ' || log_cmd || ' >> "' || log_file_path || '"''');

  ELSE
    SET log_cmd = 'ERROR: DX not running for xlinkfile_organisation';
    EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': ' || log_cmd || ' >> "' || log_file_path || '"''');
  END IF;
ELSE
  SET log_cmd = 'No records to process for xlinkfile_organisation';
  EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': ' || log_cmd || ' >> "' || log_file_path || '"''');
END IF;

GO

-- Create sql string to get purge items for opportunity
EXECUTE (REPLACE(sql, 'person', 'opportunity'));

-- Get count of records to process
SELECT COUNT(*) INTO rec_count FROM #temp_xlinkfile
WHERE parent_object_name = 'opportunity';

-- Check record is more than zero
IF rec_count != 0 THEN

-- Check XWEB is running
  return_code = CALL xinit('xlinkfile_opportunity');
  IF return_code = 0 THEN

-- Add log entry
    SET log_cmd = 'Processing ' || CONVERT(VARCHAR(12), rec_count) || ' xlinkfile_opportunity records';
    EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': ' || log_cmd || ' >> "' || log_file_path || '"''');

-- Load xlinfile_organiation table
    INSERT INTO xlinkfile_opportunity
    (xprofile_ref, xmode, xstatus, xupdate_timestamp)
    SELECT
      xprofile_ref, xmode, xstatus, xupdate_timestamp
    FROM #temp_xlinkfile
    WHERE parent_object_name = 'opportunity';

-- Run XWEB
    CALL xput('xlinkfile_opportunity', NULL, NULL, NULL, 'Y');

-- Capture results
    UPDATE #temp_xlinkfile
    SET
      x.xstatus = xlf.xstatus
      ,x.xerror_value = xlf.xerror_value
      ,x.xerror_message = xlf.xerror_message
    FROM #temp_xlinkfile x
      INNER JOIN xlinkfile_opportunity xlf ON x.xprofile_ref = xlf.xprofile_ref
    WHERE xlf.xmode = 'D'
      AND xlf.xupdate_timestamp = CAST(xtimestamp AS VARCHAR);

-- Log results
    SELECT ('Procssed xlinkfile_opportunity ' ||
      CAST(SUM(CASE xstatus WHEN 40 THEN 1 ELSE 0 END) AS VARCHAR) || ' passed, ' ||
      CAST(SUM(CASE WHEN xstatus > 40 THEN 1 ELSE 0 END) AS VARCHAR) || ' failed, ' ||
      CAST(SUM(CASE WHEN xstatus < 40 THEN 1 ELSE 0 END) AS VARCHAR) || ' unprocessed') INTO log_cmd
    FROM #temp_xlinkfile
    WHERE parent_object_name = 'opportunity';

    EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': ' || log_cmd || ' >> "' || log_file_path || '"''');

  ELSE
    SET log_cmd = 'ERROR: DX not running for xlinkfile_opportunity';
    EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': ' || log_cmd || ' >> "' || log_file_path || '"''');
  END IF;
ELSE
  SET log_cmd = 'No records to process for xlinkfile_opportunity';
  EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': ' || log_cmd || ' >> "' || log_file_path || '"''');
END IF;

GO

-- Write CSV of records purged
EXECUTE ('
UNLOAD
SELECT
  ''"linkfile_ref","parent_obj_name","parent_obj_ref",'' ||
  ''"file_name","name","parent_name","type","record_status",'' ||
  ''"create_timestamp","update_timestamp","xmode","xstatus",'' ||
  ''"xupdate","xerror_value","xerror_message"''
UNION ALL
SELECT
  ''"'' || CAST(xprofile_ref AS VARCHAR) ||
  ''","'' || parent_object_name ||
  ''","'' || CAST(parent_object_ref AS VARCHAR) ||
  ''","'' || file_name ||
  ''","'' || displayname ||
  ''","'' || parent_displayname ||
  ''","'' || type ||
  ''","'' || record_status ||
  ''","'' || create_timestamp ||
  ''","'' || update_timestamp ||
  ''","'' || xmode ||
  ''","'' || CAST(xstatus AS VARCHAR) ||
  ''","'' || xupdate_timestamp ||
  ''","'' || CAST(xerror_value AS VARCHAR) ||
  ''","'' || ISNULL(xerror_message, '''') ||
  ''"''
FROM #temp_xlinkfile
 TO ''' || csv_file_path || ''' FORMAT ASCII QUOTES off ESCAPES off;')

GO

-- All done tidy up
EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': Finished >> "' || log_file_path || '"''');

DROP VARIABLE log_file_path;
DROP VARIABLE choke_limit;
DROP VARIABLE create_months_older_than;
DROP VARIABLE update_months_older_than;
DROP VARIABLE type_exclude_list;
DROP VARIABLE xtimestamp;
DROP VARIABLE sql;
DROP VARIABLE return_code;
DROP VARIABLE csv_file_path;
DROP VARIABLE rec_count;
DROP VARIABLE log_cmd;
DROP VARIABLE var_text;

DROP TABLE #temp_xlinkfile;

GO
