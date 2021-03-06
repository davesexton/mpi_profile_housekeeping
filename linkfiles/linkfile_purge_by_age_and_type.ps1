$log_file_path = 'S:\P7UKMPDEV'
$choke_limit = 2
$create_months_older_than = 60
$update_months_older_than = 24
$type_exclude_list = 'WCRH,WPCV,WCON,WINT,YQAL'

$conn_string = "uid=xweb;pwd=xxweb;eng=devsrsysql20;"
$host_string = '10.240.104.50'
$auth_string = "SET TEMPORARY OPTION CONNECTION_AUTHENTICATION='Company=Microdec PLC;"
$auth_string += "application=Profile7-Data-Xchange-Module;"
$auth_string += "signature=000fa55157edb8e14d818eb4fe3db41447146f1571g473248bd328c2eb04a0749216a68cc08d3a2f2f2';"

clear

$type_exclude_list = ($type_exclude_list -Split ',' | % {"'$_'"}) -Join ','

$timestamp = Get-Date -Format 'yyyy-MM-dd hh:mm:ss'

$sql = @"
$auth_string;

SELECT TOP $choke_limit
  '>' AS [>]
  ,l.linkfile_ref AS xprofile_ref
  ,l.parent_object_ref
  ,l.file_name
  ,l.displayname
  ,l.type
  ,l.record_status
  ,p.displayname AS parent_displayname
  ,l.create_timestamp
  ,l.update_timestamp
  ,'D' AS xmode
  ,20 AS xstatus 
  ,CAST('$timestamp' AS DATETIME) AS [xupdate_timestamp]
  ,l.parent_object_name
  ,CAST(NULL AS INT) AS xerror_value
  ,CAST(NULL AS VARCHAR(255)) AS xerror_message
INTO #temp_xlinkfile
FROM linkfile l
  LEFT OUTER JOIN person p ON l.parent_object_ref = p.person_ref
WHERE l.parent_object_name IN('person')
  AND l.create_timestamp < DATEADD(month, -1 * $create_months_older_than, GETDATE())
  AND l.update_timestamp < DATEADD(month, -1 * $update_months_older_than, GETDATE())
  AND l.type NOT IN($type_exclude_list)
  AND l.update_timestamp < DATEADD(month, -12, GETDATE())
;
--ORDER BY l.update_timestamp;

SELECT
  '>' AS [>]
  ,xprofile_ref AS ["Linkfile Ref"]
  ,parent_object_ref
  ,type
  ,record_status
  ,'"' + CAST(create_timestamp AS VARCHAR(19)) + '"' AS create_timestamp
  ,'"' + CAST(update_timestamp AS VARCHAR(19)) + '"' AS update_timestamp
  ,xmode
  ,xstatus
  ,'"' + CAST(xupdate_timestamp AS VARCHAR(19)) + '"' AS xupdate_timestamp
  ,'"' + CAST(xerror_value AS varchar) + '"' AS xerror_value
  ,'"' + xerror_message + '"' AS xerror_message
  ,'"Error deleting existing record [Error from ue_delete_record]"' AS long_text
FROM #temp_xlinkfile;

SELECT
  '>' AS [>]
  ,xprofile_ref AS ref
  ,1 AS id
  ,CAST(parent_object_ref AS VARCHAR) AS [column_value012345678901234567890123456789]
FROM #temp_xlinkfile
UNION ALL
SELECT
  '>' AS [>]
  ,xprofile_ref AS ref
  ,2 AS id
  ,type AS column_value
FROM #temp_xlinkfile
UNION ALL
SELECT
  '>' AS [>]
  ,xprofile_ref AS ref
  ,3 AS id
  ,'Error deleting existing record Error from ue_delete_record]' AS column_value
FROM #temp_xlinkfile
UNION ALL
SELECT
  '>' AS [>]
  ,xprofile_ref AS ref
  ,4 AS id
  ,NULL AS column_value
FROM #temp_xlinkfile

;


"@
$x = dbisql -nogui -d1 -host $host_string -c $conn_string $sql

exit

$x = ($x -Split "`n" | ? {$_ -match '^>'}) | % {
    ([Regex]::Matches($_, '"[^"]+"|\S+') | % {$_.Value.Trim('"')}) -Join ','
} | ConvertFrom-Csv | Select * -ExcludeProperty '>'

$x = $x | % {$_ | Add-Member -MemberType NoteProperty -Name 'parent_object_name' -Value 'person'; $_}

$x | Format-List