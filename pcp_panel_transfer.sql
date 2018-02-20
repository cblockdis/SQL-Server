ALTER PROCEDURE Chs_pcp_panel_transfer @old_provider_id VARCHAR(36) = '%',
                                       @new_provider_id VARCHAR(36) = '%',
                                       @set_location    UNIQUEIDENTIFIER = NULL,
                                       @perc            NUMERIC(16, 10) = 100.00,
                                       @date_stop       DATETIME = NULL,
                                       @keep_unassigned BIT = 0
AS  
    DECLARE @new_provider_name VARCHAR(255) = NULL

    IF Object_id('tempdb..#zp_pcp_temp') IS NOT NULL
      DROP TABLE #zp_pcp_temp

    IF @new_provider_id != '%'
      SET @new_provider_name = (SELECT TOP 1 pm.description
                                  FROM provider_mstr pm (nolock)
                                 WHERE provider_id = @new_provider_id)

    /* Get list of records to update */
    SELECT TOP (@perc) PERCENT pat_provider_id,
                               pp.person_id,
                               pp.provider_id,
                               Cast(NULL AS VARCHAR(36))last_loc_id,
                               Cast(NULL AS VARCHAR(8)) last_pcp_dt
      INTO #zp_pcp_temp
      FROM patient_provider pp (nolock)
     WHERE pp.exp_date = ''
       AND pp.provider_id LIKE @old_provider_id
       AND pp.provider_id NOT LIKE Isnull(NULLIF(@new_provider_id, '%'), '')
       AND pp.ud_pat_prov_nbr = 6

    UPDATE z
       SET last_loc_id = ca.location_id,
           last_pcp_dt = ca.begin_date_of_service
      FROM #zp_pcp_temp z (nolock)
           CROSS APPLY(SELECT Row_number()
                                OVER (
                                  ORDER BY enc_timestamp DESC) seq,
                              c.begin_date_of_service,
                              pe.location_id
                         FROM patient_encounter pe (nolock)
                              JOIN charges c (nolock)
                                ON pe.enc_id = c.source_id
                        WHERE c.link_id IS NULL
                          AND c.service_item_id LIKE '99[23][01]_%'
                          AND z.person_id = c.person_id
                          AND c.begin_date_of_service >= CONVERT(VARCHAR(8), Dateadd(Year, -2, Getdate()), 112)) ca

    IF ( @set_location IS NOT NULL )
      DELETE FROM #zp_pcp_temp
       WHERE last_loc_id != @set_location

    DELETE FROM #zp_pcp_temp
     WHERE last_pcp_dt IS NULL

    IF @date_stop IS NOT NULL
      DELETE FROM #zp_pcp_temp
       WHERE last_pcp_dt <= CONVERT(VARCHAR(8), @date_stop, 112)

    BEGIN TRANSACTION update_panels

  BEGIN TRY
      /* Terminate existing PCP on patient_provider record(s) */
      UPDATE pp
         SET exp_date = CONVERT(VARCHAR(8), Getdate(), 112),
             reason_exp_id = 'ED74CCA1-AE6B-4764-BA7C-EA891EDA1572',--Termination
             modified_by = 1,
             modify_timestamp = Getdate()
        FROM patient_provider pp (nolock)
             JOIN #zp_pcp_temp z (nolock)
               ON pp.pat_provider_id = z.pat_provider_id

      PRINT 'Patients removed from panel: ' + Cast(@@ROWCOUNT AS VARCHAR(75))

      /* Set person pcp record to new provider */
      UPDATE p
         SET primarycare_prov_id = Isnull(@new_provider_id, t.last_provider_id),
             primarycare_prov_name = LEFT(Isnull(@new_provider_name, t.description), 30),
             modified_by = 1,
             modify_timestamp = Getdate()
        FROM person p (nolock)
             JOIN #zp_pcp_temp z (nolock)
               ON p.person_id = z.person_id
             LEFT OUTER JOIN (SELECT z.person_id,
                                     m.last_provider_id,
                                     pm.description
                                FROM #zp_pcp_temp z (nolock)
                                     JOIN mdb_lastmedenc m (nolock)
                                       ON z.person_id = m.person_id
                                     JOIN provider_practice_types ppt (nolock)
                                       ON m.last_provider_id = ppt.provider_id
                                          AND ppt.provider_type_provider6_ind = 'Y'
                                     JOIN provider_mstr pm (nolock)
                                       ON m.last_provider_id = pm.provider_id
                                          AND pm.delete_ind <> 'Y'
                                     JOIN license_detail ld (nolock)
                                       ON Cast(ppt.provider_id AS VARCHAR(36)) = ld.limit_value
                                          AND m.last_provider_id NOT LIKE CASE @old_provider_id
                                                                            WHEN '%'
                                                                            THEN '00000000-0000-0000-0000-000000000000'
                                                                            ELSE @old_provider_id
                                                                          END) t
                          ON z.person_id = t.person_id
       WHERE Isnull(@new_provider_id, t.last_provider_id) IS NOT NULL

      PRINT 'Person records updated: ' + Cast(@@ROWCOUNT AS VARCHAR(75))

      /* Create new patient_provider record(s) */
      INSERT INTO patient_provider
                  (practice_id,
                   pat_provider_id,
                   person_id,
                   provider_id,
                   reason_exp_id,
                   eff_date,
                   exp_date,
                   ud_pat_prov_nbr,
                   created_by,
                   modified_by)
      SELECT DISTINCT '0001',-- Practice ID
                      Newid(),-- Patient Provider ID
                      z.person_id,-- Person ID
                      Isnull(@new_provider_id, t.last_provider_id),-- Provider ID
                      '74F2702F-B726-4F29-B692-544C4704C9AC',-- Reason Expired (Blank description, used for tracking purposes)
                      CONVERT(VARCHAR(8), Getdate(), 112),-- Effective Date
                      '',-- Expired Date
                      '6',-- Patient Provider Specialty Number(6 for Primary Care)
                      1,-- Created By(1 for CHS Admin)
                      1 -- Modified By (Same as Created)
        FROM #zp_pcp_temp z (nolock)
             LEFT OUTER JOIN (SELECT DISTINCT z.person_id,
                                              m.last_provider_id
                                FROM #zp_pcp_temp z (nolock)
                                     JOIN mdb_lastmedenc m (nolock)
                                       ON z.person_id = m.person_id
                                     JOIN provider_practice_types ppt (nolock)
                                       ON m.last_provider_id = ppt.provider_id
                                          AND ppt.provider_type_provider6_ind = 'Y'
                                     JOIN license_detail ld (nolock)
                                       ON Cast(ppt.provider_id AS VARCHAR(36)) = ld.limit_value
                                     JOIN provider_mstr pm (nolock)
                                       ON m.last_provider_id = pm.provider_id
                                          AND pm.delete_ind <> 'Y'
                                          AND pm.provider_id NOT LIKE CASE @old_provider_id
                                                                        WHEN '%'
                                                                        THEN '00000000-0000-0000-0000-000000000000'
                                                                        ELSE @old_provider_id
                                                                      END) t
                          ON z.person_id = t.person_id
       WHERE Isnull(@new_provider_id, t.last_provider_id) IS NOT NULL

      PRINT 'Patients added to new panel(s): ' + Cast(@@ROWCOUNT AS VARCHAR(75))

      IF @keep_unassigned = 1
        BEGIN
            /* If patient was not reassigned then put them back on original provider's panel */
            UPDATE pp
               SET exp_date = '',
                   modify_timestamp = Getdate(),
                   modified_by = 1,
                   reason_exp_id = NULL
              FROM patient_provider pp (nolock)
                   JOIN #zp_pcp_temp z (nolock)
                     ON pp.pat_provider_id = z.pat_provider_id
                   LEFT OUTER JOIN patient_provider pp2 (nolock)
                                ON pp.person_id = pp2.person_id
                                   AND pp2.exp_date = ''
                                   AND pp2.ud_pat_prov_nbr = 6
             WHERE pp2.pat_provider_id IS NULL

            PRINT 'Unassigned patients kept on panel: ' + Cast(@@ROWCOUNT AS VARCHAR(75))
        END

      COMMIT TRANSACTION update_panels
  END TRY

  BEGIN CATCH
      PRINT 'Failed to update panel(s). Rolling back transaction.'

      PRINT Error_message()

      ROLLBACK TRANSACTION update_panels
  END CATCH
go
GRANT EXECUTE ON dbo.Chs_pcp_panel_transfer TO "public"
go