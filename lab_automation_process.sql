ALTER PROCEDURE Chs_lab_automation_process
AS
	/*	LAB AUTOMATION PROCESS	*****************************************************************************
	*																										*
	*	This procedure auto-generates the procedures / pending charges for lab orders in EHR Order Module.	*
	*																										*
	*	04/19/2016 -- Created Procedure	-- ZP																*
	*	07/07/2016 -- Added AEL West to Medicare rule for Memphis Locations -- ZP							*
	*   01/03/2018 -- Added exception for Medicare Payers and SIM 85610 to pass thru to EPM -- ZP			*
	********************************************************************************************************/

    DECLARE @test_run BIT = 0 -- Set to 1 to disable the procedure / pending charge inserts and enable logging
	DECLARE @enable_logging BIT = 1 -- Set to 1 if not in test mode to keep logging enabled
    DECLARE @offSet INT = 0 -- Use this to offset the date to pull labs for(-1 for previous day, 0 for current, etc). 
	PRINT Getdate() + @offSet
	DECLARE @limitLocation NVARCHAR(36) = '%' --'59BCE09D-F681-4A2F-BA3A-3DE7303F3BA5' -- Use '%' for all locations
	
	-- SELECT location_name, location_id FROM location_mstr lm (nolock)

    SET NOCOUNT ON;

    IF Object_id('tempdb..#chs_lab_temp') IS NOT NULL
      BEGIN
          DROP TABLE #chs_lab_temp
      END

    SELECT Row_number()
             OVER (
               ORDER BY ln.person_id, ln.enc_id, lt.test_code_id) AS rn,
           ln.person_id,
           ln.enc_id,
           p.first_name,
           p.last_name,
           lt.test_code_id                                        AS 'labtest_key',
           ln.order_type,
           ln.lab_id,
           lt.collection_time,
           lt.collector_id,
           pm.payer_id,
           lt.order_test_id
      INTO #chs_lab_temp
      FROM lab_order_tests lt (nolock)
           JOIN lab_nor ln (nolock)
             ON lt.order_num = ln.order_num
           LEFT OUTER JOIN order_schedule os (nolock)
                        ON ln.order_num = os.order_id
           JOIN patient_encounter pe (nolock)
             ON ln.enc_id = pe.enc_id
			    AND pe.location_id LIKE ( @limitLocation )					
           LEFT OUTER JOIN payer_mstr pm (nolock)
                        ON pe.cob1_payer_id = pm.payer_id
           JOIN person p (nolock)
             ON ln.person_id = p.person_id
     WHERE CONVERT(VARCHAR(10), ln.enc_timestamp, 112) = CONVERT(VARCHAR(10), Getdate() + @offSet, 112)
       AND lt.charge_id IS NULL -- Don't pull labs that already have a procedure submitted
       AND lt.collection_time IS NOT NULL -- Don't pull labs that have not been collected yet
       --AND os.schedule_id IS NULL -- Don't pull future labs
       AND ln.delete_ind <> 'Y' -- Don't pull deleted / canceled labs
	   AND lt.test_code_id NOT IN ( 'NG999125' ) -- Do not create procedure for test_codes in this list

    DECLARE @lastRow INTEGER = (SELECT Max(rn)
         FROM #chs_lab_temp)
    DECLARE @x INTEGER = 1
    DECLARE @person_id NVARCHAR(36)
    DECLARE @enc_id NVARCHAR(36)
    DECLARE @labtest_key NVARCHAR(75)
    DECLARE @order_type NVARCHAR(1)
    DECLARE @lab_id INTEGER
    DECLARE @sim_code NVARCHAR(12)
    DECLARE @dx_code_1 NVARCHAR(12) = NULL
    DECLARE @dx_code_2 NVARCHAR(12) = NULL
    DECLARE @dx_code_3 NVARCHAR(12) = NULL
    DECLARE @dx_code_4 NVARCHAR(12) = NULL
    DECLARE @provider_id NVARCHAR(36) = NULL
    DECLARE @location_id NVARCHAR(36) = NULL
    DECLARE @charge_id NVARCHAR(36) = NULL
    DECLARE @isError BIT = 0
    DECLARE @isBillable BIT = 1
	DECLARE @medicareOverride BIT = 0
    DECLARE @primary_payer_id NVARCHAR(36) = NULL
	DECLARE @order_test_id NVARCHAR(36) = NULL

    WHILE @x <= @lastRow
      BEGIN -- Start gathering data for insert      
          SET @person_id = NULL
          SET @enc_id = NULL
          SET @labtest_key = NULL
          SET @order_type = NULL
          SET @lab_id = NULL
          SET @sim_code = NULL
          SET @dx_code_1 = NULL
          SET @dx_code_2 = NULL
          SET @dx_code_3 = NULL
          SET @dx_code_4 = NULL
          SET @provider_id = NULL
          SET @location_id = NULL
          SET @charge_id = NULL
          SET @isError = 0
          SET @isBillable = 1
		  SET @medicareOverride = 0
          SET @primary_payer_id = NULL

          SELECT @person_id = person_id,
                 @enc_id = enc_id,
                 @labtest_key = labtest_key,
                 @order_type = order_type,
                 @lab_id = lab_id,
				 @order_test_id = order_test_id
            FROM #chs_lab_temp
           WHERE rn = @x

          SELECT @location_id = location_id,
                 @provider_id = rendering_provider_id,
                 @primary_payer_id = pe.cob1_payer_id
            FROM patient_encounter pe (nolock)
           WHERE pe.enc_id = @enc_id
             AND pe.person_id = @person_id

          IF @lab_id = 2290 -- QuestDiagnostics are billed directly to the payer.
            BEGIN
                SET @isBillable = 0
            END

          IF @lab_id IN (3270, 3271) -- AEL, Medicare / Medicare HMOs are billed directly to the payer.
             AND @primary_payer_id IN (SELECT DISTINCT pm.payer_id
                                         FROM payer_mstr pm (nolock)
                                        WHERE pm.financial_class IN ( 'F2F5A113-B8F4-4719-8517-0459F3B29138', '271052AC-BA60-4B96-ADEC-85A85ADF61F7', 'B16B7960-5637-403F-9780-409A71964736' ) )
            BEGIN
                SET @isBillable = 0
				SET @medicareOverride = 1 -- Use this flag to identify medicare payers when at pending charge phase */
            END

          /* Get the diagnosis codes mapped to the lab in order module */
          SELECT @dx_code_1 = DX1,
                 @dx_code_2 = DX2,
                 @dx_code_3 = DX3,
                 @dx_code_4 = DX4
            FROM (SELECT 'DX' + Cast(Row_number() OVER ( ORDER BY unique_diag_num DESC) AS VARCHAR(15)) AS seq,
                         lod.diagnosis_code_id                                                          AS dx_code
                    FROM lab_order_diag lod (nolock)
                         JOIN #chs_lab_temp c (nolock)
                           ON lod.order_test_id = c.order_test_id
                   WHERE c.rn = @x) t
                 PIVOT (Max(dx_code)
                       FOR seq IN (DX1,
                                   DX2,
                                   DX3,
                                   DX4) ) piv;

          /* Get the SIM code / group mapped to the labtest_key in lab_tests_xref */
          SELECT DISTINCT @sim_code = l.sim_code
            FROM lab_tests_xref l (nolock)
                 INNER JOIN external_system e (nolock)
                         ON l.system_id = e.external_system_id
                 LEFT JOIN lab_dept_xref x (nolock)
                        ON l.labdept = x.lab_dept
                 LEFT JOIN mstr_lists m (nolock)
                        ON x.ng_dept = m.mstr_list_item_id
           WHERE ng_labtest_key = @labtest_key
             AND order_type = @order_type
             AND l.system_id = @lab_id
             AND l.delete_ind <> 'Y'
					  
          DECLARE @sim_group_name NVARCHAR(75) = NULL
          DECLARE @SIM_Codes TABLE
            (
               seq             INTEGER,
               service_item_id VARCHAR(12)
            )

          /* Table does not lose it's values on redundant passthroughs so need to keep this line to make sure table is empty
             each time it loops through the cycle */
          DELETE FROM @SIM_codes

          /*  Check to see if the mapped SIM code is for a bundled lab and retrieve all relative SIM codes for that bundle. If 
              not a bundle the same process will apply but the loop will end on the first row. */
          IF EXISTS(SELECT *
                      FROM sim_groups sg (nolock)
                     WHERE sg.service_item_group_name = @sim_code)
            BEGIN
                INSERT INTO @SIM_Codes
                            (seq,
                             service_item_id)
                SELECT Row_number()
                         OVER (
                           ORDER BY service_item_id),
                       service_item_id
                  FROM sim_group_members sg (nolock)
                 WHERE sg.service_item_group_name = @sim_code

                SET @sim_group_name = @sim_code
            END
          ELSE
            BEGIN
                INSERT INTO @SIM_Codes
                            (seq,
                             service_item_id)
                     VALUES (1,
                             @sim_code)

                SET @sim_group_name = NULL
            END

          DECLARE @sim_description NVARCHAR(250) = NULL
          DECLARE @place_of_service NVARCHAR(10) = NULL
          DECLARE @cpt4_code_id NVARCHAR(25) = NULL
          DECLARE @simNbr INTEGER = 1
          DECLARE @maxNbr INTEGER = (SELECT Max(seq)
               FROM @SIM_Codes)
          DECLARE @sim_price NUMERIC(16, 2) = 0.00
          DECLARE @dx_library NVARCHAR(36) = NULL
		  DECLARE @sim_mod_1 NCHAR(2) = NULL
		  DECLARE @sim_mod_2 NCHAR(2) = NULL

          /* Get the Diagnosis Library ID for patient procedure insert */
          SET @dx_library = (SELECT TOP 1 dcm.diagnosis_code_lib_id
                               FROM diagnosis_code_mstr dcm (nolock)
                              WHERE dcm.diagnosis_code_id = @dx_code_1
                                AND dcm.delete_ind <> 'Y')

          WHILE @simNbr <= @maxNbr
            BEGIN -- Start SIM insert into patient procedure
                
                    SET @sim_code = (SELECT service_item_id
                                       FROM @SIM_Codes
                                      WHERE seq = @simNbr)
                    SET @simNbr += 1

                    /* Get relevant service item information (description, cpt4, pos and price) */
                    SELECT @sim_description = sim.description,
                           @cpt4_code_id = sim.cpt4_code_id,
                           @place_of_service = CASE
                                                 WHEN Isnull(ct.code, sim.place_of_service) = '  '
                                                 THEN NULL
                                                 ELSE Isnull(ct.code, sim.place_of_service)
                                               END,
                           @sim_price = sim.current_price,
						   @sim_mod_1 = sim.modifier_1,
						   @sim_mod_2 = sim.modifier_2 
                      FROM service_item_mstr sim (nolock)
                           JOIN cpt4_code_mstr cpt4 (nolock)
                             ON cpt4.cpt4_code_id = sim.cpt4_code_id
                           JOIN library_mstr lm (nolock)
                             ON lm.library_id = sim.service_item_lib_id
                                AND lm.delete_ind = 'N'
                           LEFT OUTER JOIN code_tables ct
                                        ON sim.place_of_service = ct.code
                                           AND ct.code_type = 'place_serv'
                                           AND ct.delete_ind = 'N'
                     WHERE service_item_lib_id = '00CB861F-B72F-47B9-8BAD-669ED330B3E2'
                       AND Rtrim(Ltrim(service_item_id)) = @sim_code
                       AND CONVERT(VARCHAR(10), Getdate(), 112) BETWEEN eff_date AND exp_date
                       AND sim.delete_ind = 'N'

                    /* If the place of service is not mapped to the SIM code then assign the location's
                    	place of service when inserting into patient procedure table */
                    IF @place_of_service IS NULL
                      BEGIN
                          SET @place_of_service = (SELECT TOP 1 lm.place_of_service
                                                     FROM location_mstr lm (nolock)
                                                    WHERE lm.location_id = @location_id)
                      END

					/* Set the defaults for output parameters in ng_add_patient_procedure */
                    DECLARE @error_code INT = 1
                    DECLARE @error_desc NVARCHAR(255) = N'ng_add_patient_procedure: (Success), Patient Procedure Record Added.'
                    DECLARE @uniq_id NVARCHAR(36) = Newid()
                    DECLARE @service_date NVARCHAR(8) = CONVERT(NVARCHAR(8), Getdate() + @offSet, 112)

					/* If more than one SIM exists in the group leave charge_id NULL to assign it a random GUID later */
                    SET @charge_id = ( CASE
                                         WHEN @maxNbr > 1
                                         THEN NULL
                                         ELSE @uniq_id
                                       END )
									   
					IF @test_run = 1
					  BEGIN 
					    SET @error_desc = 'TEST RUN: No attempt to insert patient procedure and/or pending charge.'
				      END

					/* Log the inserts to be made */
                    IF @test_run = 1 OR @enable_logging = 1
                      BEGIN
                          INSERT INTO chs_labs_process_log
                                      (person_id,
                                       enc_id,
                                       labtest_key,
                                       order_type,
                                       lab_id,
                                       sim_code,
                                       dx_code_1,
                                       dx_code_2,
                                       dx_code_3,
                                       dx_code_4,
                                       provider_id,
                                       location_id,
                                       charge_id,
                                       isError,
                                       isBillable,
                                       primary_payer_id,
                                       sim_group_name,
                                       sim_description,
                                       place_of_service,
                                       cpt4_code_id,
                                       simNbr,
                                       maxNbr,
                                       sim_price,
                                       dx_library,
                                       error_code,
                                       error_desc,
                                       uniq_id,
                                       service_date)
                               VALUES ( @person_id,
                                        @enc_id,
                                        @labtest_key,
                                        @order_type,
                                        @lab_id,
                                        @sim_code,
                                        @dx_code_1,
                                        @dx_code_2,
                                        @dx_code_3,
                                        @dx_code_4,
                                        @provider_id,
                                        @location_id,
                                        @charge_id,
                                        @isError,
                                        @isBillable,
                                        @primary_payer_id,
                                        @sim_group_name,
                                        @sim_description,
                                        @place_of_service,
                                        @cpt4_code_id,
                                        @simNbr,
                                        @maxNbr,
                                        @sim_price,
                                        @dx_library,
                                        @error_code,
                                        @error_desc,
                                        @uniq_id,
                                        @service_date )
                      END
					  
					BEGIN TRANSACTION T1 -- Start the transaction
					BEGIN TRY -- Start to check for errors
                    IF @test_run = 0
                      BEGIN /* Run only if not a test run */

                          EXEC Ng_add_patient_procedure
                            @po_result_code = @error_code output,
                            @po_result_message = @error_desc output,
                            @pi_enterprise_id = N'00001',
                            @pi_practice_id = N'0001',
                            @pi_person_id = @person_id,
                            @pi_enc_id = @enc_id,
                            @pi_created_by = 1,-- CHS Admin
                            @pi_create_timestamp_tz = 0,
                            @pio_uniq_id = @uniq_id output,
                            @pi_provider_id = @provider_id,
                            @pi_location_id = @location_id,
                            @pi_service_item_lib_id = N'00cb861f-b72f-47b9-8bad-669ed330b3e2',
                            @pi_service_item_group_name = @sim_group_name,
                            @pi_service_item_group_seq_num= @simNbr,
                            @pi_service_item_id = @sim_code,
                            @pi_service_item_desc = @sim_description,
                            @pi_service_date = @service_date,
                            @pi_cpt4_code_id = @sim_code,
                            @pi_referring_provider_id = NULL,
                            @pi_referring_provider_name = N'',
                            @pi_assisting_provider_id = NULL,
                            @pi_date_resolved = N'',
                            @pi_modifier_id_1 = @sim_mod_1,
                            @pi_modifier_id_2 = @sim_mod_2,
                            @pi_modifier_id_3 = N'',
                            @pi_modifier_id_4 = N'',
                            @pi_diagnosis_code_id_1 = @dx_code_1,
                            @pi_diagnosis_code_lib_id_1 = @dx_library,
                            @pi_diagnosis_code_id_2 = @dx_code_2,
                            @pi_diagnosis_code_lib_id_2 = @dx_library,
                            @pi_diagnosis_code_id_3 = @dx_code_3,
                            @pi_diagnosis_code_lib_id_3 = @dx_library,
                            @pi_diagnosis_code_id_4 = @dx_code_4,
                            @pi_diagnosis_code_lib_id_4 = @dx_library,
                            @pi_diagnosis_code_id_5 = N'',
                            @pi_diagnosis_code_lib_id_5 = NULL,
                            @pi_diagnosis_code_id_6 = N'',
                            @pi_diagnosis_code_lib_id_6 = NULL,
                            @pi_diagnosis_code_id_7 = N'',
                            @pi_diagnosis_code_lib_id_7 = NULL,
                            @pi_diagnosis_code_id_8 = N'',
                            @pi_diagnosis_code_lib_id_8 = NULL,
                            @pi_diagnosis_code_id_9 = N'',
                            @pi_diagnosis_code_lib_id_9 = NULL,
                            @pi_diagnosis_code_id_10 = N'',
                            @pi_diagnosis_code_lib_id_10 = NULL,
                            @pi_diagnosis_code_id_11 = N'',
                            @pi_diagnosis_code_lib_id_11 = NULL,
                            @pi_diagnosis_code_id_12 = N'',
                            @pi_diagnosis_code_lib_id_12 = NULL,
                            @pi_place_of_service = @place_of_service,
                            @pi_accept_assign_ind = N'',
                            @pi_units = 1,
                            @pi_payer_id = N'00000000-0000-0000-0000-000000000000',
                            @pi_amount = @sim_price,
                            @pi_suppress_billing_ind = N'N',
                            @pi_tooth = N'',
                            @pi_surface = N'',
                            @pi_quadrant = N'',
                            @pi_surface_descriptor = 0,
                            @pi_supernumerary_ind = N'N',
                            @pi_defective_ind = N'N',
                            @pi_not_applicable_date = NULL,
                            @pi_approval_date = NULL,
                            @pi_dental_ind = N'N',
                            @pi_delete_ind = N'N',
                            @pi_note = N'',
                            @pi_start_time = N'',
                            @pi_stop_time = N'',
                            @pi_total_time = N'',
                            @pi_base_unit = 0,
                            @pi_alt_code = N'',
                            @pi_anesthesia_billing_ind = N'N',
                            @pi_behavioral_billing_ind = N'N',
                            @pi_source_product_id = N'EHR',
                            @pi_Rx_on_file_ind = N'N',
                            @pi_national_drug_code = N'',
                            @pi_medical_director_id = NULL,
                            @pi_surgical_proc_code_id = NULL
							
						  PRINT @enc_id 
						  PRINT @sim_code
						  PRINT @sim_description 
						  /* If service marked billable send it to EPM. Medicare services with a QW modifier are 'CLIA Waved' 
						     and should be sent over to EPM. All other Medicare services should not. */
                          IF @isBillable = 1 
						  	OR (@medicareOverride = 1 
							  	AND @isBillable = 0 
								AND (@sim_code = '85610' 
								  	OR @sim_mod_1 = 'QW' 
									OR @sim_mod_2 = 'QW')) 
										BEGIN
										  /* Moved billable indicator update here so it only sets encounter to billable when actually submitting 
											 charges over to EPM */
											   
										  UPDATE patient_encounter
											 SET billable_ind = 'Y'
										   WHERE enc_id = @enc_id
										   
										   INSERT INTO pending_charge
														(practice_id,
														 enc_id,
														 uniq_id,
														 created_by,
														 create_timestamp,
														 modified_by,
														 modify_timestamp)
												 VALUES ('0001',
														 @enc_id,
														 @uniq_id,
														 1,
														 Getdate(),
														 1,
														 Getdate())
										END
                      END /* Run if not a test run */
                    COMMIT TRANSACTION T1
                END TRY -- End the check for errors
                /* If an error occurs throw error to the caller and rollback the creation of the procedure(s) */
                BEGIN CATCH
                    DECLARE @err_mess NVARCHAR(500) = Error_message()

                    RAISERROR(@err_mess,1,1)

                    ROLLBACK TRANSACTION T1

                    IF @test_run = 1 OR @enable_logging = 1
                      BEGIN
                          UPDATE chs_labs_process_log
                             SET isError = 1,
                                 error_desc = LEFT(@err_mess, 250)
                            FROM chs_labs_process_log c
                           WHERE c.person_id = @person_id
                             AND c.enc_id = @enc_id
                             AND c.labtest_key = @labtest_key
                             AND Isnull(c.sim_code,'') = Isnull(@sim_code,'')
                      END

                    SET @isError = 1
                END CATCH
            END -- End SIM insert into patient procedure

		  /* If an error occurred or this is a test run do not update the lab_order_tests record */
          IF @isError = 0
             AND @test_run = 0
            BEGIN
				/* For Sim Groups with multiple codes set the charge_id to an arbitrary GUID. */
                IF @charge_id IS NULL
                  SET @charge_id = Newid();

                UPDATE l
                   SET charge_id = @charge_id
                  FROM lab_order_tests l (nolock)
                 WHERE l.order_test_id = @order_test_id
            END
          ELSE
            BEGIN
                SET @isError = 0 -- Reset error flag
            END

          SET @isBillable = 1 -- Reset billable indicator 
          SET @x += 1 -- Go to the next lab in the list
      END -- End gathering data for insert
go
GRANT EXECUTE ON dbo.Chs_lab_automation_process TO "public"
go