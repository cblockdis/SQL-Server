 ALTER PROCEDURE [dbo].[Chs_uds_measure_depression_cms2017] @start_date DATETIME,
                                                           @end_date   DATETIME,
                                                           @uds_year   CHAR(4),
                                                           @uds_type   VARCHAR(10)=NULL
AS
    SET NOCOUNT ON;
  
    DECLARE @startDate VARCHAR(8) = CONVERT(VARCHAR(8), @start_date, 112)
    DECLARE @endDate VARCHAR(8) = CONVERT(VARCHAR(8), @end_date, 112)

    IF @uds_type IS NULL
      SET @uds_type = 'fqhc'

    IF @uds_year IS NULL
      SET @uds_year = Year(@start_date)

    IF Object_id('tempdb..#universe') IS NOT NULL
      DROP TABLE #universe

    IF Object_id('tempdb..#screenings') IS NOT NULL
      DROP TABLE #screenings

    IF Object_id('tempdb..#depressed') IS NOT NULL
      DROP TABLE #depressed
	  
	IF Object_id('tempdb..#services') IS NOT NULL
	  DROP TABLE #services 

    SELECT DISTINCT person_id,
                    date_of_birth,
                    Min(m.begin_date_of_service) start_date,
                    Cast(0 AS INTEGER)           has_diag,
                    Cast(NULL AS VARCHAR(8))     diag_date,
                    Cast(-1 AS INTEGER)          screen_result,
                    Cast(NULL AS VARCHAR(8))     screen_date,
                    Cast(0 AS INTEGER)           exclude,
                    Cast(0 AS INTEGER)           follow_up,
                    Cast(NULL AS VARCHAR(8))     follow_up_date
      INTO #universe
      FROM mdb_udsauto m (nolock)
     WHERE uds_type = @uds_type
       AND uds_year = @uds_year
       AND m.rep_group = 'MED'
     GROUP BY person_id,
              date_of_birth
    OPTION (OPTIMIZE FOR UNKNOWN)

	/* Must be 12 by the start of the reporting period */
    DELETE FROM #universe
     WHERE dbo.Chs_age(date_of_birth, @startDate) < 12

	/* Build list of all diagnoses / problems for patients */
    SELECT person_id,
           service_date,
           Cast(code AS VARCHAR(75)) code
      INTO #depressed
      FROM (SELECT uni.person_id,
                   pp.service_date,
                   pp.diagnosis_code_id_1,
                   pp.diagnosis_code_id_2,
                   pp.diagnosis_code_id_3,
                   pp.diagnosis_code_id_4,
                   pp.diagnosis_code_id_5,
                   pp.diagnosis_code_id_6,
                   pp.diagnosis_code_id_7,
                   pp.diagnosis_code_id_8,
                   pp.diagnosis_code_id_9,
                   pp.diagnosis_code_id_10,
                   pp.diagnosis_code_id_11,
                   pp.diagnosis_code_id_12
              FROM #universe uni (nolock)
                   JOIN patient_procedure pp (nolock)
                     ON uni.person_id = pp.person_id
             WHERE pp.delete_ind != 'Y'
               AND pp.amount >= 0) ta
           UNPIVOT(code
                  FOR field IN (diagnosis_code_id_1,
                                diagnosis_code_id_2,
                                diagnosis_code_id_3,
                                diagnosis_code_id_4,
                                diagnosis_code_id_5,
                                diagnosis_code_id_6,
                                diagnosis_code_id_7,
                                diagnosis_code_id_8,
                                diagnosis_code_id_9,
                                diagnosis_code_id_10,
                                diagnosis_code_id_11,
                                diagnosis_code_id_12)) piv
     WHERE Isnull(code, '') != ''

    INSERT INTO #depressed
    SELECT person_id,
           service_date,
           code
      FROM (SELECT uni.person_id,
                   c.begin_date_of_service service_date,
                   icd9cm_code_id,
                   icd9cm_code_id_2,
                   icd9cm_code_id_3,
                   icd9cm_code_id_4
              FROM #universe uni (nolock)
                   JOIN charges c (nolock)
                     ON uni.person_id = c.person_id
             WHERE c.emr_uniq_id IS NULL
               AND c.link_id IS NULL
               AND c.amt >= 0) ta
           UNPIVOT(code
                  FOR field IN (icd9cm_code_id,
                                icd9cm_code_id_2,
                                icd9cm_code_id_3,
                                icd9cm_code_id_4)) piv
     WHERE Isnull(code, '') != ''

    INSERT INTO #depressed
    SELECT uni.person_id,
           Isnull(NULLIF(pp.onset_date, ''), CONVERT(VARCHAR(8), pp.create_timestamp, 112)), -- If no onset date, use the create date
           pp.concept_id code
      FROM #universe uni (nolock)
           JOIN patient_problems pp (nolock)
             ON uni.person_id = pp.person_id

	/* Keep only diagnoses / problems for depression / bipolar disorder */
    DELETE FROM #depressed
     WHERE code NOT IN (SELECT code
                          FROM cms_value_sets_2017 c (nolock)
                         WHERE c.[value set oid] IN ( '2.16.840.1.113883.3.600.450', '2.16.840.1.113883.3.600.145' ))

	/* Flag patients in universe that have a depression or bipolar diagnosis, and pull the earliest onset date */
    UPDATE u
       SET has_diag = 1,
           diag_date = onset
      FROM #universe u (nolock)
           JOIN (SELECT uni.person_id,
                        Min(service_date) onset
                   FROM #universe uni (nolock)
                        JOIN #depressed d (nolock)
                          ON uni.person_id = d.person_id
                  GROUP BY uni.person_id) ta
             ON u.person_id = ta.person_id

	/* Pull all depression screenings(standardized). If no score found on the normal depression screening
	   then default to PHQ-9 results. */
    SELECT u.person_id,
           CONVERT(VARCHAR(8), pe.enc_timestamp, 112) screen_date,
           Isnull(curr_trmt, 0)                       exclude,
           Isnull(txt_score, CASE
                               WHEN Isnull(d.opt_item_1, 4) = 4
                                    AND Isnull(d.opt_item_2, 4) = 4
                               THEN 'negative'
                               WHEN Isnull(d.opt_item_1, 0) IN ( 1, 2, 3 )
                                     OR Isnull(d.opt_item_2, 0) IN ( 1, 2, 3 )
                               THEN 'positive'
                               ELSE NULL
                             END)                     status,
           d.txt_total_score
      INTO #screenings
      FROM #universe u (nolock)
	  	   JOIN depression_screening_short_ ds (nolock)
             ON u.person_id = ds.person_id
           LEFT OUTER JOIN CHS_Red_flag_quest_ r (nolock)
             ON r.enc_id = ds.enc_id
           JOIN patient_encounter pe (nolock)
             ON ds.enc_id = pe.enc_id           
           LEFT OUTER JOIN depression_PHQ_9_ d (nolock)
                        ON r.enc_id = d.enc_id
     WHERE pe.enc_timestamp <= getdate()
       AND Isnull(Isnull(txt_score, CASE
                                      WHEN Isnull(d.opt_item_1, 4) = 4
                                           AND Isnull(d.opt_item_2, 4) = 4
                                      THEN 'negative'
                                      WHEN Isnull(d.opt_item_1, 0) IN ( 1, 2, 3 )
                                            OR Isnull(d.opt_item_2, 0) IN ( 1, 2, 3 )
                                      THEN 'positive'
                                      ELSE NULL
                                    END), '') != ''									
									
	/* Update the universe with the most recent screening result and date of screening */
    UPDATE u
       SET screen_result = CASE status
                             WHEN 'positive'
                             THEN 1
                             ELSE 0
                           END,
           screen_date = t.screen_date
      FROM #universe u (nolock)
           JOIN (SELECT person_id,
                        Row_number()
                          OVER (
                            partition BY person_id
                            ORDER BY s.screen_date DESC) seq,
                        s.screen_date,
                        exclude,
                        status
                   FROM #screenings s (nolock)
                  WHERE s.screen_date >= '20170101') t
             ON u.person_id = t.person_id
     WHERE t.seq = 1

	/* Exclude patients who had a positive screening prior to reporting period */
    UPDATE u
       SET exclude = 1
      FROM #universe u (nolock)
           JOIN #screenings s (nolock)
             ON u.person_id = s.person_id
                AND s.screen_date < @startDate
     WHERE s.status = 'positive'
       AND u.exclude = 0

	/* Exclude patients who had a diagnosis of depression or bipolar disorder prior to
	   the first screening or visit during the reporting period */
    UPDATE u
       SET exclude = 1
      FROM #universe u (nolock)
     WHERE has_diag = 1
       AND diag_date < Isnull(u.screen_date, u.start_date)
       AND diag_date IS NOT NULL
       AND screen_date IS NOT NULL
       AND u.exclude = 0

	/* Exclude patients who were marked as 'being treated' prior to screen date or first 
	   visit of the reporting period. */
    UPDATE u
       SET exclude = 1
      FROM #universe u (nolock)
           JOIN #screenings s (nolock)
             ON u.person_id = s.person_id
     WHERE s.screen_date < Isnull(u.screen_date, u.start_date)
       AND s.exclude = 1
       AND u.exclude = 0

	/* Exclude patients that are or were on anti-depressant or bipolar medication prior
	   to screening or first visit during the reporting period. */
    UPDATE u
       SET exclude = 1
      FROM #universe u (nolock)
           JOIN patient_medication pmed (nolock)
             ON u.person_id = pmed.person_id
           JOIN measure_rxnorm_medid_xref xr (nolock)
             ON pmed.medid = xr.medid
           JOIN cms_value_sets_2017 cms (nolock)
             ON xr.rxnorm = cms.code
                AND cms.[value set oid] IN ( '2.16.840.1.113883.3.600.470', '2.16.840.1.113883.3.600.469' )
     WHERE pmed.start_date < Isnull(u.screen_date, u.start_date)
       AND u.exclude = 0

	/* Build a list of behavioral services */
    SELECT DISTINCT service_item_id
      INTO #services
      FROM service_item_mstr sim (nolock)
     WHERE ( service_item_id LIKE '9079[12]%'
          OR service_item_id LIKE '9083[247]%'
          OR service_item_id LIKE '90853%'
          OR service_item_id LIKE '96101%'
          OR service_item_id LIKE 'BHC%' )

	/* If patient was seen for a behavioral service with a depression or bipolar diagnosis
	   the same day as screening, mark follow-up as completed. */
    UPDATE u
       SET follow_up = 1,
           follow_up_date = tb.service_date
      FROM #universe u (nolock)
           JOIN (SELECT person_id,
                        dx,
                        service_date
                   FROM (SELECT uni.person_id,
                                c.icd9cm_code_id,
                                c.icd9cm_code_id_2,
                                c.icd9cm_code_id_3,
                                c.icd9cm_code_id_4,
                                c.begin_date_of_service service_date
                           FROM #universe uni (nolock)
                                JOIN charges c (nolock)
                                  ON uni.person_id = c.person_id
                                     AND uni.screen_date = c.begin_date_of_service
                          WHERE c.service_item_id IN (SELECT service_item_id
                                                        FROM #services)
                            AND uni.screen_result = 1
                            AND uni.exclude = 0
                            AND uni.follow_up = 0) ta
                        UNPIVOT(dx
                               FOR field IN (icd9cm_code_id,
                                             icd9cm_code_id_2,
                                             icd9cm_code_id_3,
                                             icd9cm_code_id_4)) piv
                  WHERE Isnull(piv.dx, '') != '') tb
             ON u.person_id = tb.person_id
           JOIN cms_value_sets_2017 c (nolock)
             ON c.Code = tb.dx
                AND c.[value set oid] IN ( '2.16.840.1.113883.3.600.450', '2.16.840.1.113883.3.600.145' )
     WHERE u.follow_up = 0
       AND u.screen_result = 1
	
    UPDATE u
       SET follow_up = 1,
           follow_up_date = tb.service_date
      FROM #universe u (nolock)
           JOIN (SELECT person_id,
                        dx,
                        service_date
                   FROM (SELECT uni.person_id,
                                pp.diagnosis_code_id_1,
                                pp.diagnosis_code_id_2,
                                pp.diagnosis_code_id_3,
                                pp.diagnosis_code_id_4,
                                pp.diagnosis_code_id_5,
                                pp.diagnosis_code_id_6,
                                pp.diagnosis_code_id_7,
                                pp.diagnosis_code_id_8,
                                pp.diagnosis_code_id_9,
                                pp.diagnosis_code_id_10,
                                pp.diagnosis_code_id_11,
                                pp.diagnosis_code_id_12,
                                pp.service_date
                           FROM #universe uni (nolock)
                                JOIN patient_procedure pp (nolock)
                                  ON uni.person_id = pp.person_id
                                     AND uni.screen_date = pp.service_date
                          WHERE pp.service_item_id IN (SELECT service_item_id
                                                         FROM #services)
                            AND uni.screen_result = 1
                            AND uni.exclude = 0
                            AND uni.follow_up = 0) ta
                        UNPIVOT(dx
                               FOR field IN (diagnosis_code_id_1,
                                             diagnosis_code_id_2,
                                             diagnosis_code_id_3,
                                             diagnosis_code_id_4,
                                             diagnosis_code_id_5,
                                             diagnosis_code_id_6,
                                             diagnosis_code_id_7,
                                             diagnosis_code_id_8,
                                             diagnosis_code_id_9,
                                             diagnosis_code_id_10,
                                             diagnosis_code_id_11,
                                             diagnosis_code_id_12)) piv
                  WHERE Isnull(piv.dx, '') != '') tb
             ON u.person_id = tb.person_id
           JOIN cms_value_sets_2017 c (nolock)
             ON c.Code = tb.dx
                AND c.[value set oid] IN ( '2.16.840.1.113883.3.600.450', '2.16.840.1.113883.3.600.145' )
     WHERE u.follow_up = 0
       AND u.screen_result = 1

	/* If a referral to a psychologist was made the date of screening then mark
	   follow-up for patient as completed. */
    UPDATE u
       SET follow_up = 1,
           follow_up_date = o.orderedDate
      FROM #universe u (nolock)
           JOIN order_ o (nolock)
             ON u.person_id = o.person_id
     WHERE actClass = 'REFR'
       AND actMood = 'ORD'
       AND actText = 'Clinical Psychology'
       AND ordered = 1
       AND o.orderedDate = u.screen_date
       AND u.follow_up = 0
       AND u.screen_result = 1

	/* If an appointment for behavioral health was scheduled on the date of screening
	   then mark follow-up for patient as completed. */
    UPDATE u
       SET follow_up = 1,
           follow_up_date = a.appt_date
      FROM #universe u (nolock)
           JOIN appointments a (nolock)
             ON u.person_id = a.person_id
                AND a.appt_date >= u.screen_date
                AND CONVERT(VARCHAR(8), a.create_timestamp, 112) = u.screen_date
                AND a.delete_ind != 'Y'
           JOIN events e (nolock)
             ON a.event_id = e.event_id
                AND e.event LIKE 'BH%'
     WHERE u.follow_up = 0
       AND u.screen_result = 1

	/* If patient was prescribed anti-depressant medication on the date of screening
	   then mark follow-up for patient as completed. */
    UPDATE u
       SET follow_up = 1,
           follow_up_date = u.screen_date
      FROM #universe u (nolock)
           JOIN patient_medication pmed (nolock)
             ON u.person_id = pmed.person_id
           JOIN measure_rxnorm_medid_xref xr (nolock)
             ON pmed.medid = xr.medid
           JOIN cms_value_sets_2017 cms (nolock)
             ON xr.rxnorm = cms.code
                AND cms.[value set oid] IN ( '2.16.840.1.113883.3.600.470', '2.16.840.1.113883.3.600.469' )
     WHERE Isnull(NULLIF(pmed.start_date, ''), CONVERT(VARCHAR(8), pmed.create_timestamp, 112)) = u.screen_date
       AND u.follow_up = 0
       AND u.screen_result = 1

	/* Set the create_timestamp for table 6b detail as current date */
    DECLARE @runDate AS VARCHAR(8) = CONVERT(VARCHAR(8), Getdate(), 112)
	
/* Used for testing 
SELECT 0 + pt.med_rec_nbr                            mrn,
       dbo.Chs_name(p.first_name, p.last_name, NULL) name,
       u.*
  FROM #universe u (nolock)
       JOIN person p (nolock)
         ON u.person_id = p.person_id
       JOIN patient pt (nolock)
         ON u.person_id = pt.person_id */	

	/* Remove current day run to prevent duplication */
    DELETE FROM chs_uds_table6b_detail
     WHERE measure = 'depression_screening'
       AND create_timestamp = @runDate 
       AND uds_year = @uds_year 
       AND uds_type = @uds_type 
       
	/* Insert records into table 6b detail. */	   
    INSERT INTO chs_uds_table6b_detail 
    		    (person_id,	 
    			 compliant,
    			 measure,
    			 measure_details,
    			 create_timestamp,
    			 uds_year,
    			 uds_type)
    SELECT u.person_id,
           CASE
             WHEN screen_result = 0
             THEN 1
             WHEN screen_result = 1
                  AND follow_up = 1
             THEN 1
             ELSE 0
           END                    compliant,
           'depression_screening' measure,
           CASE
             WHEN screen_result = 1
                  AND follow_up = 0
             THEN 'Positive depression screening found with no follow-up'
             WHEN screen_result = -1
             THEN 'No depression screening found.'
             WHEN screen_result = 0
             THEN 'Member is compliant.'
             WHEN screen_result = 1
                  AND follow_up = 1
             THEN 'Member is compliant.'
             ELSE 'Check data for discrepancies.'
           END                    measure_details,
           @runDate               rundate,
           @uds_year              udsyear,
           @uds_type              udstype
      FROM #universe u (nolock)
     WHERE u.exclude = 0
go