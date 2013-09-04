/**************************************
  Declare config variables
***************************************/
IF VAREXISTS('log_file_path') = 0 THEN
  CREATE VARIABLE log_file_path VARCHAR(900);
END IF;
IF VAREXISTS('create_months_older_than') = 0 THEN
  CREATE VARIABLE create_months_older_than INT;
END IF;
IF VAREXISTS('choke_limit') = 0 THEN
  CREATE VARIABLE choke_limit INT;
END IF;
IF VAREXISTS('record_status_include_list') = 0 THEN
  CREATE VARIABLE record_status_include_list VARCHAR(900);
END IF;

/**************************************
  Set config variables
***************************************/
SET log_file_path = 'S:\P7UKMPDEV';
SET create_months_older_than = 6;
SET choke_limit = 2;
SET record_status_include_list = 'X1,X2,X3,X4,Z';

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
IF VAREXISTS('t') = 0 THEN
  CREATE VARIABLE t VARCHAR(255);
END IF;
IF VAREXISTS('log_cmd') = 0 THEN
  CREATE VARIABLE log_cmd VARCHAR(1000);
END IF;
CREATE TABLE #temp_xopportunity 
(
  xprofile_ref INT
  ,displayname CHAR(64)
  ,type CHAR(4)
  ,record_status CHAR(4)
  ,xmode CHAR(1)
  ,xstatus INT
  ,xupdate_timestamp TIMESTAMP
  ,xerror_value INT
  ,xerror_message VARCHAR(255)
);

-- Create timestamp, log file name and CSV name
SET xtimestamp = CAST(LEFT(CAST(GETDATE() AS VARCHAR), 16) AS DATETIME);
SET csv_file_path = log_file_path + '\housekeeping_opportunity_purge' 
   + REPLACE(REPLACE(REPLACE(LEFT(CAST(xtimestamp AS VARCHAR), 16), '-', ''), ':', ''), ' ', '')
   + '.csv';
SET log_file_path = log_file_path + '\housekeeping_opportunity_purge' 
   + REPLACE(REPLACE(REPLACE(LEFT(CAST(xtimestamp AS VARCHAR), 16), '-', ''), ':', ''), ' ', '')
   + '.log';

-- Make sure choke limit is not above max
IF choke_limit > 32767 THEN
  SET choke_limit = 32767;
END IF;

-- Log variables
EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': Variable CHOKE_LIMIT =^> '|| CONVERT(VARCHAR(12), choke_limit) || ' >> "' || log_file_path || '"''');
EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': Variable CREATE_MONTH_OLDER_THAN =^> '|| CONVERT(VARCHAR(12), create_months_older_than) || ' >> "' || log_file_path || '"''');
EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': Variable RECORD_STATUS_INCLUDE_LIST =^> '|| record_status_include_list || ' >> "' || log_file_path || '"''');
EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': Variable LOG_FILE_PATH =^> '|| log_file_path || ' >> "' || log_file_path || '"''');
EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': Variable CSV_FILE_PATH =^> '|| csv_file_path || ' >> "' || log_file_path || '"''');   
   
-- Create sql string to get purge items
SET sql = '
INSERT INTO #temp_xopportunity
(xprofile_ref, displayname, type, record_status,
 xmode, xstatus, xupdate_timestamp, xerror_value, xerror_message)
SELECT TOP ' || CONVERT(VARCHAR(8), choke_limit) || ' 
  o.opportunity_ref AS xprofile_ref
  ,o.displayname
  ,o.type
  ,o.record_status
  ,''D'' AS xmode
  ,20 AS xstatus 
  ,CAST( ''' || CAST(xtimestamp AS VARCHAR(16)) || ''' AS DATETIME) AS xupdate_timestamp
  ,CAST(NULL AS INT) AS xerror_value
  ,CAST(NULL AS VARCHAR(255)) AS xerror_message
FROM opportunity o
WHERE o.create_timestamp <= DATEADD(MONTH, -1 * ' || CAST(-1 * create_months_older_than AS VARCHAR) || ', GETDATE())
  AND o.create_timestamp <= DATEADD(MONTH, -1 * 12, GETDATE())
  AND o.record_status IN(''' + REPLACE(record_status_include_list, ',', ''',''') + ''') 
  NOT EXISTS (SELECT 1
              FROM event e
              WHERE e.opportunity_ref IS NOT NULL
                AND e.opportunity_ref = o.opportunity_ref)
';

EXECUTE (sql);

-- Get count of records to process
SELECT COUNT(*) INTO rec_count FROM #temp_xopportunity;

-- Check record is more than zero
IF rec_count != 0 THEN

-- Check XWEB is running
  return_code = CALL xinit('xopportunity');
  IF return_code = 0 THEN

-- Add log entry
    SET log_cmd = 'Processing ' || CONVERT(VARCHAR(12), rec_count) || ' xopportunity records';
    EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': ' || log_cmd || ' >> "' || log_file_path || '"''');
    
-- Load xlinfile_person table
    INSERT INTO xopportunity
    (xprofile_ref, xmode, xstatus, xupdate_timestamp)
    SELECT
      xprofile_ref, xmode, xstatus, xupdate_timestamp
    FROM #temp_xopportunity;
    
-- Run XWEB
    CALL xput('xopportunity', null, null, null,'y');

-- Capture results
    UPDATE #temp_xopportunity
    SET 
      x.xstatus = xlf.xstatus
      ,x.xerror_value = xlf.xerror_value
      ,x.xerror_message = xlf.xerror_message  
    FROM #temp_xopportunity x
      INNER JOIN xopportunity xlf ON x.xprofile_ref = xlf.xprofile_ref
    WHERE xlf.xmode = 'D'
      AND xlf.xupdate_timestamp = CAST(xtimestamp AS VARCHAR);
    
-- Log results
    SELECT ('Procssed xopportunity ' || 
      CAST(SUM(CASE xstatus WHEN 40 THEN 1 ELSE 0 END) AS VARCHAR) || ' passed, ' ||
      CAST(SUM(CASE WHEN xstatus > 40 THEN 1 ELSE 0 END) AS VARCHAR) || ' failed, ' ||
      CAST(SUM(CASE WHEN xstatus < 40 THEN 1 ELSE 0 END) AS VARCHAR) || ' unprocessed') INTO log_cmd
    FROM #temp_xopportunity;
    
    EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': ' || log_cmd || ' >> "' || log_file_path || '"''');
 
  ELSE 
    SET log_cmd = 'ERROR: DX not running for xopportunity';
    EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': ' || log_cmd || ' >> "' || log_file_path || '"''');
  END IF;
ELSE 
  SET log_cmd = 'No records to process for xopportunity';
  EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': ' || log_cmd || ' >> "' || log_file_path || '"''');
END IF;

GO
  
-- Write CSV of records purged
SET log_cmd = '"opportunity_ref",' ||             
              '"displayname",' ||
              '"type","record_status","xmode","xstatus","xupdate","xerror_value",' || 
              '"xerror_message"';
EXECUTE ('xp_cmdshell ''echo ' || log_cmd || ' >> "' || csv_file_path || '"''');

SELECT
  CAST(xprofile_ref AS VARCHAR) AS xprofile_ref
  ,displayname
  ,type
  ,record_status
  ,xmode
  ,CAST(xstatus AS VARCHAR) AS xstatus
  ,xupdate_timestamp
  ,CAST(xerror_value AS VARCHAR) AS xerror_value
  ,ISNULL(xerror_message, '') AS xerror_message
FROM #temp_xopportunity;

OUTPUT TO 'S:\temp.csv' FORMAT ASCII DELIMITED BY ',' QUOTE '"';
EXECUTE ('xp_cmdshell ''type "S:\temp.csv" >> "' || csv_file_path || '"''');
EXECUTE ('xp_cmdshell ''del "S:\temp.csv"''');

GO

-- All done tidy up
EXECUTE ('xp_cmdshell ''echo ' || CAST(GETDATE() AS VARCHAR(23)) || ': Finished >> "' || log_file_path || '"''');

DROP VARIABLE log_file_path;
DROP VARIABLE choke_limit;
DROP VARIABLE create_months_older_than;
DROP VARIABLE record_status_include_list;

DROP VARIABLE xtimestamp;
DROP VARIABLE sql;
DROP VARIABLE parent_name;
DROP VARIABLE return_code;
DROP VARIABLE csv_file_path;
DROP VARIABLE rec_count;
DROP VARIABLE log_cmd;

DROP TABLE #temp_xopportunity;

GO
