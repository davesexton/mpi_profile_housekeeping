PRINT 'STARTING: person'
TRUNCATE TABLE person
INSERT INTO person
SELECT 
  person_ref
  ,external_ref
  ,last_name
  ,first_name
  ,title
  ,initials
  ,salutation
  ,displayname
  ,mobile_telno
  ,email_address
  ,CASE WHEN date_of_birth <= CAST('1753-01-01 12:00:00' AS DATETIME)
     THEN CAST('1753-01-01 12:00:00' AS DATETIME)
     WHEN date_of_birth >= CAST('9999-12-31 23:59:59' AS DATETIME)
     THEN CAST('9999-12-31 23:59:59' AS DATETIME)
     ELSE date_of_birth END AS date_of_birth
  ,date_of_birth_mode
  ,gender
  ,nationality
  ,ethnicity
  ,source
  ,name_key
  ,soundex_key
  ,deduplication_key
  ,responsible_user
  ,responsible_team
  ,CASE WHEN create_timestamp <= CAST('1753-01-01 12:00:00' AS DATETIME)
     THEN CAST('1753-01-01 12:00:00' AS DATETIME)
     WHEN create_timestamp >= CAST('9999-12-31 23:59:59' AS DATETIME)
     THEN CAST('9999-12-31 23:59:59' AS DATETIME)
     ELSE create_timestamp END AS create_timestamp
  ,create_user
  ,CASE WHEN update_timestamp <= CAST('1753-01-01 12:00:00' AS DATETIME)
     THEN CAST('1753-01-01 12:00:00' AS DATETIME)
     WHEN update_timestamp >= CAST('9999-12-31 23:59:59' AS DATETIME)
     THEN CAST('9999-12-31 23:59:59' AS DATETIME)
     ELSE update_timestamp END AS update_timestamp
  ,update_user
  ,record_status
  ,block_level
  ,block_user
  ,CASE WHEN block_timestamp <= CAST('1753-01-01 12:00:00' AS DATETIME)
     THEN CAST('1753-01-01 12:00:00' AS DATETIME)
     WHEN block_timestamp >= CAST('9999-12-31 23:59:59' AS DATETIME)
     THEN CAST('9999-12-31 23:59:59' AS DATETIME)
     ELSE block_timestamp END AS block_timestamp
  ,block_message
  ,record_number
  ,lock_user
  ,custom_cols_start
  ,qualification_note
  ,year_qualified
  ,driver
  ,own_car
  ,sole_agency
  ,discretion_reqd
  ,marital_status
  ,CASE WHEN cv_last_updated <= CAST('1753-01-01 12:00:00' AS DATETIME)
     THEN CAST('1753-01-01 12:00:00' AS DATETIME)
     WHEN cv_last_updated >= CAST('9999-12-31 23:59:59' AS DATETIME)
     THEN CAST('9999-12-31 23:59:59' AS DATETIME)
     ELSE cv_last_updated END AS cv_last_updated
  ,user_text1
  ,user_text2
  ,user_text3
  ,user_number1
  ,user_number2
  ,user_number3
  ,CASE WHEN user_date1 <= CAST('1753-01-01 12:00:00' AS DATETIME)
     THEN CAST('1753-01-01 12:00:00' AS DATETIME)
     WHEN user_date1 >= CAST('9999-12-31 23:59:59' AS DATETIME)
     THEN CAST('9999-12-31 23:59:59' AS DATETIME)
     ELSE user_date1 END AS user_date1
  ,CASE WHEN user_date2 <= CAST('1753-01-01 12:00:00' AS DATETIME)
     THEN CAST('1753-01-01 12:00:00' AS DATETIME)
     WHEN user_date2 >= CAST('9999-12-31 23:59:59' AS DATETIME)
     THEN CAST('9999-12-31 23:59:59' AS DATETIME)
     ELSE user_date2 END AS user_date2
  ,CASE WHEN user_date3 <= CAST('1753-01-01 12:00:00' AS DATETIME)
     THEN CAST('1753-01-01 12:00:00' AS DATETIME)
     WHEN user_date3 >= CAST('9999-12-31 23:59:59' AS DATETIME)
     THEN CAST('9999-12-31 23:59:59' AS DATETIME)
     ELSE user_date3 END AS user_date3
  ,day_telno
  ,web_site_url
  ,xexternal_ref
  ,CASE WHEN user_date4 <= CAST('1753-01-01 12:00:00' AS DATETIME)
     THEN CAST('1753-01-01 12:00:00' AS DATETIME)
     WHEN user_date4 >= CAST('9999-12-31 23:59:59' AS DATETIME)
     THEN CAST('9999-12-31 23:59:59' AS DATETIME)
     ELSE user_date4 END AS user_date4
  ,user_user1
  ,web_site2_url
  ,CASE WHEN z_last_candidate_action <= CAST('1753-01-01 12:00:00' AS DATETIME)
     THEN CAST('1753-01-01 12:00:00' AS DATETIME)
     WHEN z_last_candidate_action >= CAST('9999-12-31 23:59:59' AS DATETIME)
     THEN CAST('9999-12-31 23:59:59' AS DATETIME)
     ELSE z_last_candidate_action END AS z_last_candidate_action
  ,CASE WHEN z_last_contact_action <= CAST('1753-01-01 12:00:00' AS DATETIME)
     THEN CAST('1753-01-01 12:00:00' AS DATETIME)
     WHEN z_last_contact_action >= CAST('9999-12-31 23:59:59' AS DATETIME)
     THEN CAST('9999-12-31 23:59:59' AS DATETIME)
     ELSE z_last_contact_action END AS z_last_contact_action
  ,compliance_category
  ,compliance_status
  ,CASE WHEN compliance_expiry <= CAST('1753-01-01 12:00:00' AS DATETIME)
     THEN CAST('1753-01-01 12:00:00' AS DATETIME)
     WHEN compliance_expiry >= CAST('9999-12-31 23:59:59' AS DATETIME)
     THEN CAST('9999-12-31 23:59:59' AS DATETIME)
     ELSE compliance_expiry END AS compliance_expiry
  ,compliance_notes
  ,compliance_update_user
  ,CASE WHEN compliance_update_timestamp <= CAST('1753-01-01 12:00:00' AS DATETIME)
     THEN CAST('1753-01-01 12:00:00' AS DATETIME)
     WHEN compliance_update_timestamp >= CAST('9999-12-31 23:59:59' AS DATETIME)
     THEN CAST('9999-12-31 23:59:59' AS DATETIME)
     ELSE compliance_update_timestamp END AS compliance_update_timestamp
  ,do_not_mailshot
FROM P7nalivecopy..profile.person
PRINT 'COMPLETED: person'
GO

