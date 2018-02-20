DECLARE @startDate VARCHAR(8) = '20180101'
DECLARE @endDate VARCHAR(8) = '20180130'
DECLARE @currentDate VARCHAR(8) = CONVERT(VARCHAR(8), Getdate(), 112)
DECLARE @dxStartDate VARCHAR(8) = CONVERT(VARCHAR(8), Dateadd(Month, -8, Getdate()), 112)
 
IF Object_id('tempdb..#appts') IS NOT NULL
  DROP TABLE #appts
 
PRINT @startDate
 
PRINT @endDate
 
PRINT @currentDate
 
SELECT Row_number()
         OVER (
           partition BY a.person_id
           ORDER BY a.appt_date DESC, a.begintime DESC) seq,
       a.appt_id,
       a.person_id,
       a.appt_date,
       a.begintime,
       a.appt_kept_ind,
       Cast(0 AS INTEGER)                               has_tenncare,
       Cast(0 AS INTEGER)                               has_medicare,
       Cast(0 AS INTEGER)                               has_commercial,
       Cast(0.00 AS NUMERIC(19, 2))                    family_income,
       Cast(0 AS SMALLINT)                              family_size,
       Cast(NULL AS CHAR(1))                            income_interval,
       Cast(NULL AS CHAR(1))                            refused_ind,
       Cast(NULL AS VARCHAR(8))                         inc_eff_date,
       Cast(NULL AS VARCHAR(8))                         inc_exp_date,
       Cast(NULL AS VARCHAR(12))                        last_qual_dx,
       Cast(NULL AS VARCHAR(8))                         last_qual_dx_date,
       Cast(NULL AS VARCHAR(1000))                      last_qual_dx_desc
  INTO #appts
  FROM appointments a (nolock)
       LEFT OUTER JOIN chs_report_exclusions cre (nolock)
                    ON a.person_id = cre.person_id
 WHERE a.delete_ind <> 'Y'
   AND a.cancel_ind <> 'Y'
   AND a.resched_ind <> 'Y'
   AND a.appt_date BETWEEN @startDate AND @endDate
   AND a.person_id IS NOT NULL
   AND cre.person_id IS NULL
 
UPDATE #appts
   SET last_qual_dx = tb.dx,
       last_qual_dx_date = tb.service_date,
       last_qual_dx_desc = dcm.description
  FROM #appts
       JOIN (SELECT Row_number()
                      OVER (
                        partition BY person_id
                        ORDER BY service_date DESC) rn,
                    person_id,
                    dx,
                    service_date
               FROM (SELECT DISTINCT person_id,
                                     dx,
                                     service_date
                       FROM (SELECT DISTINCT pp.person_id,
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
                               FROM #appts a (nolock)
                                    JOIN patient_procedure pp (nolock)
                                      ON a.person_id = pp.person_id
                                    JOIN service_item_mstr sim (nolock)
                                      ON pp.service_item_id = sim.service_item_id
                                    JOIN mstr_lists ml (nolock)
                                      ON sim.department = ml.mstr_list_item_id
                              WHERE ml.mstr_list_item_desc = 'BH'
                                AND pp.service_date BETWEEN @dxStartDate AND @currentDate
                                AND pp.delete_ind <> 'Y') t
                            UNPIVOT (dx
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
                                                  diagnosis_code_id_12)) p
                            JOIN chs_sn_diagnosis_mstr dm (nolock)
                              ON p.dx = dm.code
                      WHERE dx <> ''
                     UNION
                     SELECT DISTINCT person_id,
                                     dx,
                                     service_date
                       FROM (SELECT DISTINCT c.person_id,
                                             c.icd9cm_code_id,
                                             c.icd9cm_code_id_2,
                                             c.icd9cm_code_id_3,
                                             c.icd9cm_code_id_4,
                                             c.begin_date_of_service service_date
                               FROM #appts a (nolock)
                                    JOIN charges c (nolock)
                                      ON a.person_id = c.person_id
                                    JOIN service_item_mstr sim (nolock)
                                      ON c.service_item_id = sim.service_item_id
                                    JOIN mstr_lists ml (nolock)
                                      ON sim.department = ml.mstr_list_item_id
                              WHERE sim.fqhc_enc_ind = 'Y'
                                AND ml.mstr_list_item_desc = 'BH'
                                AND c.link_id IS NULL
                                AND c.amt >= 0
                                AND c.begin_date_of_service BETWEEN @dxStartDate AND @currentDate) t
                            UNPIVOT (dx
                                    FOR field IN (icd9cm_code_id,
                                                  icd9cm_code_id_2,
                                                  icd9cm_code_id_3,
                                                  icd9cm_code_id_4)) p
                            JOIN chs_sn_diagnosis_mstr dm (nolock)
                              ON p.dx = dm.code
                      WHERE dx <> '') ta) tb
         ON tb.person_id = #appts.person_id
       JOIN diagnosis_code_mstr dcm (nolock)
         ON tb.dx = dcm.diagnosis_code_id
 WHERE tb.rn = 1
 
DELETE FROM #appts
 WHERE last_qual_dx IS NULL
 
UPDATE a
   SET has_medicare = 1
  FROM #appts a (nolock)
       JOIN person_payer pp (nolock)
         ON a.person_id = pp.person_id
       JOIN payer_mstr pm (nolock)
         ON pp.payer_id = pm.payer_id
       JOIN mstr_lists ml (nolock)
         ON pm.financial_class = ml.mstr_list_item_id
 WHERE ml.mstr_list_item_desc IN ( 'Medicare FQHC', 'Medicare HMO' )
   AND ( a.appt_date >= pp.policy_eff_date
          OR pp.policy_efF_date IS NULL
          OR pp.policy_eff_date = '' )
   AND ( a.appt_date <= pp.policy_exp_date
          OR pp.policy_exp_date IS NULL
          OR pp.policy_exp_date = '' )
   AND pp.active_ind = 'Y'
 
UPDATE a
   SET has_tenncare = 1
  FROM #appts a (nolock)
       JOIN person_payer pp (nolock)
         ON a.person_id = pp.person_id
       JOIN payer_mstr pm (nolock)
         ON pp.payer_id = pm.payer_id
       JOIN mstr_lists ml (nolock)
         ON pm.financial_class = ml.mstr_list_item_id
 WHERE ml.mstr_list_item_desc IN ( 'MD Tenncare', 'Medicaid', 'DD Tenncare' )
   AND ( a.appt_date >= pp.policy_eff_date
          OR pp.policy_efF_date IS NULL
          OR pp.policy_eff_date = '' )
   AND ( a.appt_date <= pp.policy_exp_date
          OR pp.policy_exp_date IS NULL
          OR pp.policy_exp_date = '' )
   AND pp.active_ind = 'Y'
 
UPDATE a
   SET has_commercial = 1
  FROM #appts a (nolock)
       JOIN person_payer pp (nolock)
         ON a.person_id = pp.person_id
       JOIN payer_mstr pm (nolock)
         ON pp.payer_id = pm.payer_id
       JOIN mstr_lists ml (nolock)
         ON pm.financial_class = ml.mstr_list_item_id
 WHERE ml.mstr_list_item_desc IN ( 'Commercial', 'Safety Net', 'SBG Special Billing Group' )
   AND ( a.appt_date >= pp.policy_eff_date
          OR pp.policy_efF_date IS NULL
          OR pp.policy_eff_date = '' )
   AND ( a.appt_date <= pp.policy_exp_date
          OR pp.policy_exp_date IS NULL
          OR pp.policy_exp_date = '' )
   AND pp.active_ind = 'Y'
 
DELETE FROM #appts
 WHERE has_medicare + has_tenncare + has_commercial > 0
 
UPDATE a
   SET family_income = fi.family_income,
       income_interval = fi.family_income_interval,
       family_size = fi.family_size_nbr,
       inc_eff_date = fi.eff_date,
       inc_exp_date = fi.exp_date,
       refused_ind = fi.refused_to_report_ind
  FROM #appts a (nolock)
       JOIN practice_person_family_info fi (nolock)
         ON a.person_id = fi.person_id
 WHERE fi.eff_date <= CONVERT(VARCHAR(8), Getdate(), 112)
   AND fi.exp_date >= CONVERT(VARCHAR(8), Getdate(), 112)
 
UPDATE #appts
   SET family_income = NULL,
       family_size = NULL
 WHERE income_interval IS NULL
 
DELETE FROM #appts
 WHERE refused_ind = 'Y'
 
SELECT 0 + pt.med_rec_nbr                                              MRN,
       dbo.Chs_name(p.first_name, p.last_name, NULL)                   Patient,
        
       CONVERT(VARCHAR(50),Cast(a.appt_date + ' ' + dbo.Chs_time(a.begintime) AS DATETIME),100) ApptDate,
       CASE
         WHEN a.appt_date < @currentDate
         THEN 'Kept'
         ELSE 'Pending'
       END                                                             Status,
       e.event                                                         ApptType,
       r.description                                                   ApptResource,
       Replace(lm.location_name, 'CHS ', '')                           Location,
       CONVERT(NUMERIC(19,2),CASE
         WHEN income_interval = 'M'
         THEN family_income * 12
         ELSE family_income
       END )                                                           AnnualIncome,
       family_size                                                     FamilySize,
       Cast(inc_eff_date AS DATE)                                      IncEffDate,
       Cast(inc_exp_date AS DATE)                                      IncExpDate,
       last_qual_dx                                                    LastQualDX,
       last_qual_dx_desc                                               DXDescription,
       Cast(last_qual_dx_date AS DATE)                                 DXDate
  FROM #appts a (nolock)
       JOIN patient pt (nolock)
         ON a.person_id = pt.person_id
       JOIN person p (nolock)
         ON a.person_id = p.person_id
       JOIN appointments ap (nolock)
         ON a.appt_id = ap.appt_id
       JOIN events e (nolock)
         ON ap.event_id = e.event_id
       JOIN appointment_members am (nolock)
         ON a.appt_id = am.appt_id
       JOIN resources r (nolock)
         ON am.resource_id = r.resource_id
       JOIN location_mstr lm (nolock)
         ON ap.location_id = lm.location_id
 WHERE seq = 1
   AND lm.location_id LIKE '%'
   AND lm.location_subgrouping1_id NOT LIKE 'ed7a0c9a-d2ae-4df2-92f1-eb77d0e6314c'   
   AND ( ( a.appt_date < @currentDate
           AND a.appt_kept_ind = 'Y' )
          OR a.appt_date >= @currentDate )
   AND dbo.Chs_age(p.date_of_birth, Getdate()) > 18
   AND CASE
         WHEN income_interval = 'M'
         THEN Isnull(family_income, 0) * 12
         ELSE Isnull(family_income, 0)
       END <= CASE family_size
                WHEN 1
                THEN 11880
                WHEN 2
                THEN 16020
                WHEN 3
                THEN 20160
                WHEN 4
                THEN 24300
                WHEN 5
                THEN 28440
                WHEN 6
                THEN 32580
                WHEN 7
                THEN 36730
                WHEN 8
                THEN 40890
                ELSE 40890 + ( ( Isnull(family_size, 8) - 8 ) * 4160 )
              END
 ORDER BY lm.location_name,
          Status DESC,
          a.appt_date ASC,
          Isnull(a.family_income, 999999999) ASC,
          a.begintime ASC
