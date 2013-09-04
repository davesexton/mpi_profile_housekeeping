/**************************************
  Declare config variables
***************************************/
IF VAREXISTS('log_file_path') = 0 THEN
  CREATE VARIABLE log_file_path VARCHAR(900);
END IF;
IF VAREXISTS('choke_limit') = 0 THEN
  CREATE VARIABLE choke_limit INT;
END IF;
IF VAREXISTS('keyword_list') = 0 THEN
  CREATE VARIABLE keyword_list VARCHAR(255);
END IF;

/**************************************
  Set config variables
***************************************/
SET log_file_path = 'S:\P7UKMPDEV';
SET choke_limit = 2;
SET keyword_list = 'delete,do not use,error,duplicate';

/**************************************
 Declare script variables
***************************************/
IF VAREXISTS('xtimestamp') = 0 THEN
  CREATE VARIABLE xtimestamp DATETIME;
END IF;
IF VAREXISTS('sql') = 0 THEN
  CREATE VARIABLE sql VARCHAR(4000);
END IF;
IF VAREXISTS('parent_name') = 0 THEN
  CREATE VARIABLE parent_name VARCHAR(30);
END IF;
IF VAREXISTS('return_code') = 0 THEN
  CREATE VARIABLE return_code INT;
END IF;
IF VAREXISTS('csv_file_path') = 0 THEN
  CREATE VARIABLE csv_file_path VARCHAR(900);
END IF;
IF VAREXISTS('rec_count') = 0 THEN
  CREATE VARIABLE rec_count INT;
END IF;
IF VAREXISTS('log_cmd') = 0 THEN
  CREATE VARIABLE log_cmd VARCHAR(1000);
END IF;
CREATE TABLE #temp_xlinkfile 
(
  xprofile_ref INT
  ,parent_object_ref INT
  ,file_name CHAR(256)
  ,displayname CHAR(64)
  ,type CHAR(4)
  ,record_status CHAR(4)
  ,parent_displayname CHAR(64)
  ,xmode CHAR(1)
  ,xstatus INT
  ,xupdate_timestamp TIMESTAMP
  ,parent_object_name VARCHAR(255)
  ,xerror_value INT
  ,xerror_message VARCHAR(255)
);
CREATE TABLE #keywords 
(
  keyword VARCHAR(255)
);

-- Create timestamp, log file name and CSV name
SET xtimestamp = CAST(LEFT(CAST(GETDATE() AS VARCHAR), 16) AS DATETIME);
SET csv_file_path = log_file_path + '\housekeeping_linkfile_purge_on_keyword' 
   + REPLACE(REPLACE(REPLACE(LEFT(CAST(xtimestamp AS VARCHAR), 16), '-', ''), ':', ''), ' ', '')
   + '.csv';
SET log_file_path = log_file_path + '\housekeeping_linkfile_purge_on_keyword' 
   + REPLACE(REPLACE(REPLACE(LEFT(CAST(xtimestamp AS VARCHAR), 16), '-', ''), ':', ''), ' ', '')
   + '.log';

-- Load keywords
SET keyword_list = keyword_list + ',';

WHILE CHARINDEX(',', keyword_list) > 0 LOOP
  IF CHARINDEX('%', keyword_list) = 0 AND LENGTH(keyword_list) > 4 THEN
    INSERT INTO #keywords (keyword)
    VALUES('%' + TRIM(SUBSTRING(keyword_list, 1, CHARINDEX(',', keyword_list) -1)) + '%');
  END IF;
  SET keyword_list = SUBSTRING(keyword_list, CHARINDEX(',', keyword_list) + 1, LENGTH(keyword_list));
END LOOP;

-- Make sure choke limit is not above max
IF choke_limit > 32767 THEN
  SET choke_limit = 32767;
END IF;

-- Start log file
EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': Starting Linkfile Purge Displayname Keyword >> "' || log_file_path || '"''');

-- Log variables
EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': Variable CHOKE_LIMIT =^> '|| CONVERT(VARCHAR(12), choke_limit) || ' >> "' || log_file_path || '"''');
EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': Variable LOG_FILE_PATH =^> '|| log_file_path || ' >> "' || log_file_path || '"''');
EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': Variable CSV_FILE_PATH =^> '|| csv_file_path || ' >> "' || log_file_path || '"''');

-- Create sql string to get purge items for PERSON
SET sql = '
INSERT INTO #temp_xlinkfile
(xprofile_ref, parent_object_ref, file_name, displayname, type, record_status, parent_displayname,
 xmode, xstatus, xupdate_timestamp, parent_object_name, xerror_value, xerror_message)
SELECT TOP ' || CONVERT(VARCHAR(8), choke_limit) || ' 
  l.linkfile_ref AS xprofile_ref
  ,l.parent_object_ref
  ,l.file_name
  ,l.displayname
  ,l.type
  ,l.record_status
  ,p.displayname AS parent_displayname
  ,''D'' AS xmode
  ,20 AS xstatus 
  ,CAST( ''' || CAST(xtimestamp AS VARCHAR(16)) || ''' AS DATETIME) AS xupdate_timestamp
  ,l.parent_object_name
  ,CAST(NULL AS INT) AS xerror_value
  ,CAST(NULL AS VARCHAR(255)) AS xerror_message
FROM linkfile l
  INNER JOIN #keywords ON l.displayname LIKE keyword
  LEFT OUTER JOIN person p ON l.parent_object_ref = p.person_ref
WHERE l.parent_object_name IN(''person'')
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
    CALL xput('xlinkfile_person', null, null, null,'y');

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
      CAST(SUM(CASE xstatus WHEN 40 THEN 1 ELSE 0 END) AS VARCHAR) || ' passed, ' ||
      CAST(SUM(CASE WHEN xstatus > 40 THEN 1 ELSE 0 END) AS VARCHAR) || ' failed, ' ||
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
    CALL xput('xlinkfile_organisation', null, null, null, 'y');

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
    CALL xput('xlinkfile_opportunity', null, null, null,'y');

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
SET log_cmd = '"linkfile_ref","parent_object_name","parent_object_ref",' ||             
              '"file_name","displayname","parent_displayname",' ||
              '"type","record_status","xmode","xstatus","xupdate","xerror_value",' || 
              '"xerror_message"';
EXECUTE ('xp_cmdshell ''echo ' || log_cmd || ' >> "' || csv_file_path || '"''');

GO

SELECT
  CAST(xprofile_ref AS VARCHAR) AS xprofile_ref
  ,parent_object_name
  ,CAST(parent_object_ref AS VARCHAR)
  ,file_name
  ,displayname
  ,parent_displayname
  ,type
  ,record_status
  ,xmode
  ,CAST(xstatus AS VARCHAR) AS xstatus
  ,xupdate_timestamp
  ,CAST(xerror_value AS VARCHAR) AS xerror_value
  ,ISNULL(xerror_message, '') AS xerror_message
FROM #temp_xlinkfile;

OUTPUT TO 'S:\temp.csv' FORMAT ASCII DELIMITED BY ',' QUOTE '"';

GO

EXECUTE ('xp_cmdshell ''type "S:\temp.csv" >> "' || csv_file_path || '"''');
EXECUTE ('xp_cmdshell ''del "S:\temp.csv"''');

GO

-- All done tidy up
EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': Finished >> "' || log_file_path || '"''');

DROP VARIABLE log_file_path;
DROP VARIABLE choke_limit;
DROP VARIABLE keyword_list;

DROP VARIABLE xtimestamp;
DROP VARIABLE sql;
DROP VARIABLE parent_name;
DROP VARIABLE return_code;
DROP VARIABLE csv_file_path;
DROP VARIABLE rec_count;
DROP VARIABLE log_cmd;

DROP TABLE #temp_xlinkfile;
DROP TABLE #keywords;

GO
