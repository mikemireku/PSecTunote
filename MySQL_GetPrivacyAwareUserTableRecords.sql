CREATE DEFINER=`root`@`localhost` PROCEDURE `GetPrivacyAwareUserTableRecords`(IN PatientId INT, IN AttributeCategoryDataId INT, IN DataAccessorId INT, IN DataInfoPrivacyId INT, IN DataRetentionValue DATETIME, IN DataGranularityPrivacyId INT, IN DataVisibilityPrivacyId INT, IN DataPurposePrivacyId INT, OUT outquery VARCHAR (65500))
BEGIN
	DECLARE TableName VARCHAR(45) DEFAULT '';
	DECLARE sql_query VARCHAR (65500) DEFAULT '';
    DECLARE sql_query_attribute VARCHAR (255) DEFAULT '';
    DECLARE sql_patient_id VARCHAR (45) DEFAULT '';
    DECLARE sql_query_attribute_count INT DEFAULT 0;
    DECLARE purpose_counter INT DEFAULT 0;
    DECLARE third_party_flag INT DEFAULT 0;
    DECLARE done INT DEFAULT FALSE;
    DECLARE privacy_policy_flag INT DEFAULT TRUE;
    
    DECLARE attribute_category_data_privacy_flag INT DEFAULT FALSE;
    DECLARE data_info_privacy_flag INT DEFAULT FALSE;
    DECLARE retention_privacy_flag INT DEFAULT FALSE;
    
    DECLARE granularity_privacy_flag_1 INT DEFAULT FALSE;
    DECLARE granularity_privacy_flag_2 INT DEFAULT FALSE;
    DECLARE granularity_privacy_flag_3 INT DEFAULT FALSE;
    
    DECLARE visibility_privacy_flag_1 INT DEFAULT FALSE;
    DECLARE visibility_privacy_flag_2 INT DEFAULT FALSE;
    DECLARE visibility_privacy_flag_3 INT DEFAULT FALSE;
    DECLARE visibility_privacy_flag_4 INT DEFAULT FALSE;
    
    DECLARE purpose_privacy_flag_1 INT DEFAULT FALSE;
    DECLARE purpose_privacy_flag_2 INT DEFAULT FALSE;
    DECLARE purpose_privacy_flag_3 INT DEFAULT FALSE;
    DECLARE purpose_privacy_flag_4 INT DEFAULT FALSE;
    DECLARE purpose_privacy_flag_5 INT DEFAULT FALSE;
    
    DECLARE data_accessor_security_class VARCHAR (255) DEFAULT '';
    DECLARE data_info_privacy_class VARCHAR (255) DEFAULT '';
    
    DECLARE curAttribute CURSOR FOR
	SELECT attribute_name FROM tunote_ppdb.attribute_data WHERE table_name = TableName AND idattribute_category_data = AttributeCategoryDataId;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    
    -- PRIVACY TESTING -------------------------------------------
    -- 1. Attribute-Category-Data-TableName
    IF (AttributeCategoryDataId = 1 OR AttributeCategoryDataId = 2) THEN
		SET TableName = 'consent';
        SET sql_patient_id = 'patient_id';
    ELSEIF (AttributeCategoryDataId = 6) THEN
        SET TableName = 'requisition';
        SET sql_patient_id = 'patient_id';
    ELSEIF (AttributeCategoryDataId = 3 OR AttributeCategoryDataId = 4 OR AttributeCategoryDataId = 5 OR AttributeCategoryDataId = 6) THEN
		SET TableName = 'patient';
        SET sql_patient_id = 'idPatient';
    END IF;
    
    -- 2. Attribute-Category-Data-Privacy-Policy
    SELECT COUNT(attribute_name) INTO sql_query_attribute_count
    FROM tunote_ppdb.attribute_data WHERE table_name = TableName AND idattribute_category_data = AttributeCategoryDataId;
    
    IF (sql_query_attribute_count <> 0) THEN
		SET attribute_category_data_privacy_flag = TRUE;
	ELSE
		SET attribute_category_data_privacy_flag = FALSE;
    END IF;
    
    -- 3. Data-Info-Privacy-Policy
	SELECT data_security_classification_level INTO data_accessor_security_class 
    FROM tunote_ppdb.data_accessor_profile WHERE iddata_accessor = DataAccessorId;
    
    SELECT security_classification_level INTO data_info_privacy_class 
    FROM tunote_ppdb.data_information_privacy_policy WHERE iddata_information_privacy_policy = DataInfoPrivacyId;
    
    IF (trim(data_accessor_security_class) = trim(data_info_privacy_class)) THEN
		SET data_info_privacy_flag = TRUE;
	ELSE
		SET data_info_privacy_flag = FALSE;
	END IF;
    
    -- 4. Data-Retention-Privacy-Policy
    IF (CURDATE() < DataRetentionValue) THEN
		SET retention_privacy_flag = TRUE;
	END IF;
    
    -- 5. Data-Granularity-Privacy-Policy
    IF (DataGranularityPrivacyId = 1) THEN
        SET granularity_privacy_flag_1 = TRUE;
	ELSEIF (DataGranularityPrivacyId = 2) THEN
        SET granularity_privacy_flag_2 = TRUE;
    ELSEIF (DataGranularityPrivacyId = 3) THEN
        SET granularity_privacy_flag_3 = TRUE;
	ELSE
		SET privacy_policy_flag = FALSE;
    END IF;
    
    -- 6. Data-Visibility-Privacy-Policy
    SELECT purpose_count, third_party_access INTO purpose_counter, third_party_flag FROM tunote_ppdb.privacy_statistics;
    
    IF (DataVisibilityPrivacyId = 1) THEN
        SET visibility_privacy_flag_1 = TRUE;
	ELSEIF (DataVisibilityPrivacyId = 2) THEN
        SET visibility_privacy_flag_2 = TRUE;
	ELSEIF (DataVisibilityPrivacyId = 3 AND third_party_flag = 1) THEN
        SET visibility_privacy_flag_3 = TRUE;
    ELSEIF (DataVisibilityPrivacyId = 4) THEN
        SET visibility_privacy_flag_4 = TRUE;
	ELSE
		SET privacy_policy_flag = FALSE;
    END IF;
    
    -- 7. Data-Purpose-Privacy-Policy
    IF (DataPurposePrivacyId = 1 AND purpose_counter = 0) THEN
		SET purpose_privacy_flag_1 = TRUE;
	ELSEIF (DataPurposePrivacyId = 2) THEN
        SET purpose_privacy_flag_2 = TRUE;
    ELSEIF (DataPurposePrivacyId = 3) THEN
        SET purpose_privacy_flag_3 = TRUE;
    ELSEIF (DataPurposePrivacyId = 4) THEN
        SET purpose_privacy_flag_4 = TRUE;
    ELSEIF (DataPurposePrivacyId = 5) THEN
        SET purpose_privacy_flag_5 = TRUE;
	ELSE
		SET privacy_policy_flag = FALSE;
    END IF;
    -- END-OF-PRIVACY TESTING --------------------------------------------
    
    -- PRIVACY ATTRIBUTE_DATA RETRIEVAL -----------------------------------
    IF (attribute_category_data_privacy_flag AND data_info_privacy_flag AND retention_privacy_flag AND granularity_privacy_flag_1 AND (visibility_privacy_flag_1 OR visibility_privacy_flag_2 OR visibility_privacy_flag_4) AND (purpose_privacy_flag_2 OR purpose_privacy_flag_3 OR purpose_privacy_flag_4 OR purpose_privacy_flag_5) AND privacy_policy_flag) THEN
		
		OPEN curAttribute;
		SET sql_query_attribute = NULL;
		
		getAttribute_loop: LOOP
			FETCH curAttribute INTO sql_query_attribute;
			IF done THEN 
				LEAVE getAttribute_loop;
			END IF;
			
			SET sql_query = CONCAT(sql_query, sql_query_attribute, ', ');
			SET sql_query_attribute = NULL;
			
		END LOOP getAttribute_loop;
		CLOSE curAttribute;
	 
		SET sql_query = CONCAT('SELECT ', SUBSTRING(sql_query, 1, LENGTH(sql_query)-2), ' FROM tunote_ngs_genomics.', TableName, ' WHERE ', sql_patient_id,' = ', PatientId ,'');
		SET outquery = sql_query;
        
    ELSEIF (attribute_category_data_privacy_flag AND data_info_privacy_flag AND retention_privacy_flag AND granularity_privacy_flag_2 AND (visibility_privacy_flag_1 OR visibility_privacy_flag_2 OR visibility_privacy_flag_4) AND (purpose_privacy_flag_2 OR purpose_privacy_flag_3 OR purpose_privacy_flag_4 OR purpose_privacy_flag_5) AND privacy_policy_flag) THEN
		
		OPEN curAttribute;
		SET sql_query_attribute = NULL;
		
		getAttribute_loop: LOOP
			FETCH curAttribute INTO sql_query_attribute;
			IF done THEN 
				LEAVE getAttribute_loop;
			END IF;
			
            SET sql_query_attribute = CONCAT('SUBSTRING(',sql_query_attribute,',1,3) AS ', sql_query_attribute);
			SET sql_query = CONCAT(sql_query, sql_query_attribute, ', ');
			SET sql_query_attribute = NULL;
			
		END LOOP getAttribute_loop;
		CLOSE curAttribute;
	 
		SET sql_query = CONCAT('SELECT ', SUBSTRING(sql_query, 1, LENGTH(sql_query)-2), ' FROM tunote_ngs_genomics.', TableName, ' WHERE ', sql_patient_id,' = ', PatientId ,'');
		SET outquery = sql_query;
    
    ELSEIF (attribute_category_data_privacy_flag AND data_info_privacy_flag AND retention_privacy_flag AND granularity_privacy_flag_3 AND (visibility_privacy_flag_1 OR visibility_privacy_flag_2 OR visibility_privacy_flag_4) AND (purpose_privacy_flag_2 OR purpose_privacy_flag_3 OR purpose_privacy_flag_4 OR purpose_privacy_flag_5) AND privacy_policy_flag) THEN
		
		OPEN curAttribute;
		SET sql_query_attribute = NULL;
		
		getAttribute_loop: LOOP
			FETCH curAttribute INTO sql_query_attribute;
			IF done THEN 
				LEAVE getAttribute_loop;
			END IF;
			
            SET sql_query_attribute = CONCAT('"YES" AS ', sql_query_attribute);
			SET sql_query = CONCAT(sql_query, sql_query_attribute, ', ');
			SET sql_query_attribute = NULL;
			
		END LOOP getAttribute_loop;
		CLOSE curAttribute;
	 
		SET sql_query = CONCAT('SELECT ', SUBSTRING(sql_query, 1, LENGTH(sql_query)-2), ' FROM tunote_ngs_genomics.', TableName, ' WHERE ', sql_patient_id,' = ', PatientId ,'');
		SET outquery = sql_query;
	
    ELSEIF (attribute_category_data_privacy_flag AND data_info_privacy_flag AND retention_privacy_flag AND granularity_privacy_flag_1 AND visibility_privacy_flag_3 AND (purpose_privacy_flag_2 OR purpose_privacy_flag_3 OR purpose_privacy_flag_4 OR purpose_privacy_flag_5) AND privacy_policy_flag) THEN
		
		OPEN curAttribute;
		SET sql_query_attribute = NULL;
		
		getAttribute_loop: LOOP
			FETCH curAttribute INTO sql_query_attribute;
			IF done THEN 
				LEAVE getAttribute_loop;
			END IF;
			
			SET sql_query = CONCAT(sql_query, sql_query_attribute, ', ');
			SET sql_query_attribute = NULL;
			
		END LOOP getAttribute_loop;
		CLOSE curAttribute;
	 
		SET sql_query = CONCAT('SELECT ', SUBSTRING(sql_query, 1, LENGTH(sql_query)-2), ' FROM tunote_ngs_genomics.', TableName, ' WHERE ', sql_patient_id,' = ', PatientId ,'');
		SET outquery = sql_query;
        
    ELSEIF (attribute_category_data_privacy_flag AND data_info_privacy_flag AND retention_privacy_flag AND granularity_privacy_flag_2 AND visibility_privacy_flag_3 AND (purpose_privacy_flag_2 OR purpose_privacy_flag_3 OR purpose_privacy_flag_4 OR purpose_privacy_flag_5) AND privacy_policy_flag) THEN
		
		OPEN curAttribute;
		SET sql_query_attribute = NULL;
		
		getAttribute_loop: LOOP
			FETCH curAttribute INTO sql_query_attribute;
			IF done THEN 
				LEAVE getAttribute_loop;
			END IF;
			
            SET sql_query_attribute = CONCAT('SUBSTRING(',sql_query_attribute,',1,3) AS ', sql_query_attribute);
			SET sql_query = CONCAT(sql_query, sql_query_attribute, ', ');
			SET sql_query_attribute = NULL;
			
		END LOOP getAttribute_loop;
		CLOSE curAttribute;
	 
		SET sql_query = CONCAT('SELECT ', SUBSTRING(sql_query, 1, LENGTH(sql_query)-2), ' FROM tunote_ngs_genomics.', TableName, ' WHERE ', sql_patient_id,' = ', PatientId ,'');
		SET outquery = sql_query;
    
    ELSEIF (attribute_category_data_privacy_flag AND data_info_privacy_flag AND retention_privacy_flag AND granularity_privacy_flag_3 AND visibility_privacy_flag_3 AND (purpose_privacy_flag_2 OR purpose_privacy_flag_3 OR purpose_privacy_flag_4 OR purpose_privacy_flag_5) AND privacy_policy_flag) THEN
		
		OPEN curAttribute;
		SET sql_query_attribute = NULL;
		
		getAttribute_loop: LOOP
			FETCH curAttribute INTO sql_query_attribute;
			IF done THEN 
				LEAVE getAttribute_loop;
			END IF;
			
            SET sql_query_attribute = CONCAT('"YES" AS ', sql_query_attribute);
			SET sql_query = CONCAT(sql_query, sql_query_attribute, ', ');
			SET sql_query_attribute = NULL;
			
		END LOOP getAttribute_loop;
		CLOSE curAttribute;
	 
		SET sql_query = CONCAT('SELECT ', SUBSTRING(sql_query, 1, LENGTH(sql_query)-2), ' FROM tunote_ngs_genomics.', TableName, ' WHERE ', sql_patient_id,' = ', PatientId ,'');
		SET outquery = sql_query;
    
    ELSEIF (attribute_category_data_privacy_flag AND data_info_privacy_flag AND retention_privacy_flag AND granularity_privacy_flag_1 AND (visibility_privacy_flag_1 OR visibility_privacy_flag_2 OR visibility_privacy_flag_4) AND purpose_privacy_flag_1 AND privacy_policy_flag) THEN
		
		OPEN curAttribute;
		SET sql_query_attribute = NULL;
		
		getAttribute_loop: LOOP
			FETCH curAttribute INTO sql_query_attribute;
			IF done THEN 
				LEAVE getAttribute_loop;
			END IF;
			
			SET sql_query = CONCAT(sql_query, sql_query_attribute, ', ');
			SET sql_query_attribute = NULL;
			
		END LOOP getAttribute_loop;
		CLOSE curAttribute;
	 
		SET sql_query = CONCAT('SELECT ', SUBSTRING(sql_query, 1, LENGTH(sql_query)-2), ' FROM tunote_ngs_genomics.', TableName, ' WHERE ', sql_patient_id,' = ', PatientId ,'');
		SET outquery = sql_query;
        
    ELSEIF (attribute_category_data_privacy_flag AND data_info_privacy_flag AND retention_privacy_flag AND granularity_privacy_flag_2 AND (visibility_privacy_flag_1 OR visibility_privacy_flag_2 OR visibility_privacy_flag_4) AND purpose_privacy_flag_1 AND privacy_policy_flag) THEN
		
		OPEN curAttribute;
		SET sql_query_attribute = NULL;
		
		getAttribute_loop: LOOP
			FETCH curAttribute INTO sql_query_attribute;
			IF done THEN 
				LEAVE getAttribute_loop;
			END IF;
			
            SET sql_query_attribute = CONCAT('SUBSTRING(',sql_query_attribute,',1,3) AS ', sql_query_attribute);
			SET sql_query = CONCAT(sql_query, sql_query_attribute, ', ');
			SET sql_query_attribute = NULL;
			
		END LOOP getAttribute_loop;
		CLOSE curAttribute;
	 
		SET sql_query = CONCAT('SELECT ', SUBSTRING(sql_query, 1, LENGTH(sql_query)-2), ' FROM tunote_ngs_genomics.', TableName, ' WHERE ', sql_patient_id,' = ', PatientId ,'');
		SET outquery = sql_query;
    
    ELSEIF (attribute_category_data_privacy_flag AND data_info_privacy_flag AND retention_privacy_flag AND granularity_privacy_flag_3 AND (visibility_privacy_flag_1 OR visibility_privacy_flag_2 OR visibility_privacy_flag_4) AND purpose_privacy_flag_1 AND privacy_policy_flag) THEN
		
		OPEN curAttribute;
		SET sql_query_attribute = NULL;
		
		getAttribute_loop: LOOP
			FETCH curAttribute INTO sql_query_attribute;
			IF done THEN 
				LEAVE getAttribute_loop;
			END IF;
			
            SET sql_query_attribute = CONCAT('"YES" AS ', sql_query_attribute);
			SET sql_query = CONCAT(sql_query, sql_query_attribute, ', ');
			SET sql_query_attribute = NULL;
			
		END LOOP getAttribute_loop;
		CLOSE curAttribute;
	 
		SET sql_query = CONCAT('SELECT ', SUBSTRING(sql_query, 1, LENGTH(sql_query)-2), ' FROM tunote_ngs_genomics.', TableName, ' WHERE ', sql_patient_id,' = ', PatientId ,'');
		SET outquery = sql_query;
	
    ELSEIF (attribute_category_data_privacy_flag AND data_info_privacy_flag AND retention_privacy_flag AND granularity_privacy_flag_1 AND visibility_privacy_flag_3 AND purpose_privacy_flag_1 AND privacy_policy_flag) THEN
		
		OPEN curAttribute;
		SET sql_query_attribute = NULL;
		
		getAttribute_loop: LOOP
			FETCH curAttribute INTO sql_query_attribute;
			IF done THEN 
				LEAVE getAttribute_loop;
			END IF;
			
			SET sql_query = CONCAT(sql_query, sql_query_attribute, ', ');
			SET sql_query_attribute = NULL;
			
		END LOOP getAttribute_loop;
		CLOSE curAttribute;
	 
		SET sql_query = CONCAT('SELECT ', SUBSTRING(sql_query, 1, LENGTH(sql_query)-2), ' FROM tunote_ngs_genomics.', TableName, ' WHERE ', sql_patient_id,' = ', PatientId ,'');
		SET outquery = sql_query;
        
    ELSEIF (attribute_category_data_privacy_flag AND data_info_privacy_flag AND retention_privacy_flag AND granularity_privacy_flag_2 AND visibility_privacy_flag_3 AND purpose_privacy_flag_1 AND privacy_policy_flag) THEN
		
		OPEN curAttribute;
		SET sql_query_attribute = NULL;
		
		getAttribute_loop: LOOP
			FETCH curAttribute INTO sql_query_attribute;
			IF done THEN 
				LEAVE getAttribute_loop;
			END IF;
			
            SET sql_query_attribute = CONCAT('SUBSTRING(',sql_query_attribute,',1,3) AS ', sql_query_attribute);
			SET sql_query = CONCAT(sql_query, sql_query_attribute, ', ');
			SET sql_query_attribute = NULL;
			
		END LOOP getAttribute_loop;
		CLOSE curAttribute;
	 
		SET sql_query = CONCAT('SELECT ', SUBSTRING(sql_query, 1, LENGTH(sql_query)-2), ' FROM tunote_ngs_genomics.', TableName, ' WHERE ', sql_patient_id,' = ', PatientId ,'');
		SET outquery = sql_query;
    
    ELSEIF (attribute_category_data_privacy_flag AND data_info_privacy_flag AND retention_privacy_flag AND granularity_privacy_flag_3 AND visibility_privacy_flag_3 AND purpose_privacy_flag_1 AND privacy_policy_flag) THEN
		
		OPEN curAttribute;
		SET sql_query_attribute = NULL;
		
		getAttribute_loop: LOOP
			FETCH curAttribute INTO sql_query_attribute;
			IF done THEN 
				LEAVE getAttribute_loop;
			END IF;
			
            SET sql_query_attribute = CONCAT('"YES" AS ', sql_query_attribute);
			SET sql_query = CONCAT(sql_query, sql_query_attribute, ', ');
			SET sql_query_attribute = NULL;
			
		END LOOP getAttribute_loop;
		CLOSE curAttribute;
	 
		SET sql_query = CONCAT('SELECT ', SUBSTRING(sql_query, 1, LENGTH(sql_query)-2), ' FROM tunote_ngs_genomics.', TableName, ' WHERE ', sql_patient_id,' = ', PatientId ,'');
		SET outquery = sql_query;
    
    ELSE
		SET outquery = 'Data Privacy Policy Preferences Restriction on Information Retrieval';
    END IF;
    
    -- PREPARE stmt FROM @sql_query;
	-- EXECUTE stmt;
	-- DEALLOCATE PREPARE stmt;
    
    -- END-OF-PRIVACY ATTRIBUTE_DATA RETRIEVAL -----------------------------------
END