setlocal EnableDelayedExpansion
@echo off
set "DSN=P7MPUK"
set "LOG_FILE_PATH=S:\LOGS"
set "CHOKE_LIMIT=1"
set "CREATE_MONTHS_OLDER_THAN=60"
set "UPDATE_MONTHS_OLDER_THAN=24"
set "TYPE_EXCLUDE_LIST=WCRH,WPCV,WCON,WINT,YQAL"

:: +---------------------------------------------------------------------------+
:: |  PROFILE LINKFILE PURGE BY AGE AND TYPE                                   |
:: |  AUTHOR: Dave Sexton                                                      |
:: |  VERSION: 1.0                                                             |
:: |                                                                           |
:: |  VARIABLES:                                                               |
:: |  DSN: The name of an ODBC data source that uses the Adaptive Anywhere     |
:: |       Sybase driver. The credentials must the same as those used by DX    |
:: |  LOG_FILE_PATH: A file location to store log files, that is reachable     |
:: |                 from the Sybase database.                                 |
:: |  CHOKE_LIMIT: The maximum number of records to be purged per entity,      |
:: |               used to avoid over loading the server. Can be set for any   |
:: |               amount but will be capped at 32,767 (a Sybase limitation).  |
:: |  CREATE_MONTHS_OLDER_THAN: Records will only be purged if their create    |
:: |                            timestamp is older than this value in months.  |
:: |  UPDATE_MONTHS_OLDER_THAN: Records will only be purged if their update    |
:: |                            timestamp is older than this value in months.  |
:: |  TYPE_EXCLUDE_LIST: Comma separated list of linkfile types that are       |
:: |                     NOT to be purged.                                     |
:: +---------------------------------------------------------------------------+

:: Prepare variables
if %LOG_FILE_PATH:~-1%==\ set LOG_FILE_PATH=%LOG_FILE_PATH:~0,-1%
if %CHOKE_LIMIT% gtr 32767 set CHOKE_LIMIT=32767
set "TYPEEX='%TYPE_EXCLUDE_LIST:,=','%'"
set "XTS=%date:~6,4%-%date:~3,2%-%date:~0,2% %time:~0,2%:%time:~3,2%:00"
set "XTS=%XTS:  = 0%"
set "TS=%date:~6,4%%date:~3,2%%date:~0,2%%time:~0,2%%time:~3,2%00"
set "TS=%TS: =0%"
set "CSV=%LOG_FILE_PATH%\housekeeping_linkfile_purge_by_age_and_type_person_%TS%.csv"
set "LOG=%LOG_FILE_PATH%\housekeeping_linkfile_purge_by_age_and_type_%TS%.log"
set ^"LOG_CMD=CALL xp_cmdshell( ^
 'echo ' + CAST(GETDATE() AS VARCHAR(23)) + ': # ^>^> ' + CHAR(34) + '%LOG%' + CHAR(34)); ^"

:: Create log file
set ^"SQL=BEGIN ^
 !LOG_CMD:#=Starting Linkfile Purge by Age and Type! ^
 !LOG_CMD:#=Variable CHOKE_LIMIT = %CHOKE_LIMIT%! ^
 !LOG_CMD:#=Variable CREATE_MONTHS_OLDER_THAN = %CREATE_MONTHS_OLDER_THAN%! ^
 !LOG_CMD:#=Variable UPDATE_MONTHS_OLDER_THAN = %UPDATE_MONTHS_OLDER_THAN%! ^
 !LOG_CMD:#=Variable TYPE_EXCLUDE_LIST = %TYPE_EXCLUDE_LIST%! ^
 !LOG_CMD:#=Variable LOG_FILE_PATH = %LOG%! ^
 !LOG_CMD:#=Variable CSV_FILE_PATH = %CSV%! ^
 END; ^"

:: Start log file
dbisql -nogui -odbc -datasource %DSN% "%SQL%"

:: Create entity SQL
set ^"SQL=BEGIN ^
 DECLARE return_code INT; ^
 DECLARE rec_count INT; ^
 DECLARE log_cmd VARCHAR(1000); ^
 DECLARE q VARCHAR(1); ^
 CREATE TABLE #temp_xlinkfile ^
 (xprofile_ref INT ^
  ,parent_object_ref INT ^
  ,file_name CHAR(256) ^
  ,displayname CHAR(64) ^
  ,type CHAR(4) ^
  ,record_status CHAR(4) ^
  ,parent_displayname CHAR(64) ^
  ,create_timestamp DATETIME ^
  ,update_timestamp DATETIME ^
  ,xmode CHAR(1) ^
  ,xstatus INT ^
  ,xupdate_timestamp TIMESTAMP ^
  ,parent_object_name VARCHAR(255) ^
  ,xerror_value INT ^
  ,xerror_message VARCHAR(255)); ^
 SET q = CHAR(34); ^
 return_code = CALL xinit('xlinkfile_person'); ^
 IF return_code ^<^> 0 THEN ^
   !LOG_CMD:#=ERROR: DX not running for xlinkfile_person! ^
   RETURN; ^
 END IF; ^
 INSERT INTO #temp_xlinkfile ^
 SELECT TOP %CHOKE_LIMIT% ^
   l.linkfile_ref AS xprofile_ref ^
   ,l.parent_object_ref ^
   ,l.file_name ^
   ,REPLACE(l.displayname, q, q + q) AS displayname ^
   ,l.type ^
   ,l.record_status ^
   ,p.displayname AS parent_displayname ^
   ,l.create_timestamp ^
   ,l.update_timestamp ^
   ,'D' AS xmode ^
   ,20 AS xstatus ^
   ,CAST('%XTS%' AS DATETIME) AS xupdate_timestamp ^
   ,l.parent_object_name ^
   ,CAST(NULL AS INT) AS xerror_value ^
   ,CAST(NULL AS VARCHAR(255)) AS xerror_message ^
 FROM linkfile l ^
   LEFT OUTER JOIN person p ON l.parent_object_ref = p.person_ref ^
 WHERE l.parent_object_name IN('person') ^
   AND l.create_timestamp ^< DATEADD(month,-%CREATE_MONTHS_OLDER_THAN%, GETDATE()) ^
   AND l.update_timestamp ^< DATEADD(month,-%UPDATE_MONTHS_OLDER_THAN%, GETDATE()) ^
   AND l.type NOT IN(%TYPEEX%) ^
   AND l.update_timestamp ^< DATEADD(month,-12, GETDATE()) ^
 ORDER BY l.update_timestamp; ^
 SELECT COUNT(*) INTO rec_count FROM #temp_xlinkfile ^
 WHERE parent_object_name = 'person'; ^
 IF rec_count = 0 THEN ^
   !LOG_CMD:#=No records to process for xlinkfile_person! ^
   RETURN; ^
 END IF; ^
 !LOG_CMD:#=Processing ' + CONVERT(VARCHAR(12), rec_count) + ' xlinkfile_person records! ^
 INSERT INTO xlinkfile_person ^
 (xprofile_ref, xmode, xstatus, xupdate_timestamp) ^
 SELECT xprofile_ref, xmode, xstatus, xupdate_timestamp ^
 FROM #temp_xlinkfile ^
 WHERE parent_object_name = 'person'; ^
 CALL xput('xlinkfile_person', NULL, NULL, NULL, 'Y'); ^
 UPDATE #temp_xlinkfile ^
 SET ^
   x.xstatus = xlf.xstatus ^
   ,x.xerror_value = xlf.xerror_value ^
   ,x.xerror_message = xlf.xerror_message ^
 FROM #temp_xlinkfile x ^
   INNER JOIN xlinkfile_person xlf ON x.xprofile_ref = xlf.xprofile_ref ^
 WHERE xlf.xmode = 'D' ^
   AND xlf.xupdate_timestamp = CAST('%XTS%' AS DATETIME); ^
 SELECT ('Procssed xlinkfile_person ' + ^
   CAST(SUM(CASE WHEN xstatus ^>= 40 AND xerror_message IS NULL ^
     THEN 1 ELSE 0 END) AS VARCHAR) + ' passed, ' + ^
   CAST(SUM(CASE WHEN xstatus ^>= 40 AND xerror_message IS NOT NULL ^
     THEN 1 ELSE 0 END) AS VARCHAR) + ' failed, ' + ^
   CAST(SUM(CASE WHEN xstatus ^< 40 ^
     THEN 1 ELSE 0 END) AS VARCHAR) + ' unprocessed') INTO log_cmd ^
 FROM #temp_xlinkfile ^
 WHERE parent_object_name = 'person'; ^
 !LOG_CMD:#=' + log_cmd + '! ^
 UNLOAD SELECT ^
   q + REPLACE('linkfile_ref,parent_obj_name,parent_obj_ref,' + ^
   'file_name,name,parent_name,type,record_status,' + ^
   'create_timestamp,update_timestamp,xmode,xstatus,' + ^
   'xupdate,xerror_value,xerror_message', ',', q + ',' + q) + q ^
 UNION ALL ^
 SELECT ^
  q + CAST(xprofile_ref AS VARCHAR) + ^
  q + ',' + q + parent_object_name + ^
  q + ',' + q + CAST(parent_object_ref AS VARCHAR) + ^
  q + ',' + q + file_name + ^
  q + ',' + q + displayname + ^
  q + ',' + q + parent_displayname + ^
  q + ',' + q + type + ^
  q + ',' + q + record_status + ^
  q + ',' + q + CAST(create_timestamp AS VARCHAR) + ^
  q + ',' + q + CAST(update_timestamp AS VARCHAR) + ^
  q + ',' + q + xmode + ^
  q + ',' + q + CAST(xstatus AS VARCHAR) + ^
  q + ',' + q + CAST(xupdate_timestamp AS VARCHAR) + ^
  q + ',' + q + CAST(xerror_value AS VARCHAR) + ^
  q + ',' + q + ISNULL(xerror_message, '') + q ^
 FROM #temp_xlinkfile ^
 TO '%CSV%' FORMAT ASCII QUOTES off ESCAPES off; ^
 END; ^"

:: Run person
dbisql -nogui -odbc -datasource %DSN% "%SQL%"

:: Run organisation
dbisql -nogui -odbc -datasource %DSN% "!SQL:person=organisation!"

:: Run opportunity
dbisql -nogui -odbc -datasource %DSN% "!SQL:person=opportunity!"

:: Close log
set ^"SQL=BEGIN ^
 !LOG_CMD:#=Finished! ^
 END; ^"

dbisql -nogui -odbc -datasource %DSN% "%SQL%"

::pause


