ALTER FUNCTION dbo.Chs_days_supply(@uniq_id NVARCHAR(36))
RETURNS VARCHAR(75)
  BEGIN      
      DECLARE @returnValue VARCHAR(50) = NULL
      DECLARE @poeostrid NUMERIC(10, 0) = NULL
      DECLARE @custom_dosage_id NVARCHAR(36) = NULL
      DECLARE @rx_quantity VARCHAR(10) = NULL
      DECLARE @freqLow NUMERIC(3, 0) = NULL
      DECLARE @intLow NUMERIC(3, 0) = NULL
      DECLARE @dosLow NUMERIC(12, 4) = NULL
      DECLARE @dosForm NUMERIC(12, 4) = NULL
      DECLARE @intUnit VARCHAR(30) = NULL
	  DECLARE @rxRefills VARCHAR(10) = NULL

      SELECT @poeostrid = pm.poeostrid,
             @custom_dosage_id = pm.custom_dosage_order_id,
             @rx_quantity = pm.rx_quanity,
			 @rxRefills = pm.rx_refills
        FROM patient_medication pm (nolock)
       WHERE pm.uniq_id = @uniq_id

      IF ( @custom_dosage_id IS NULL
            OR @custom_dosage_id = '00000000-0000-0000-0000-000000000000' )
         AND ( Isnull(@poeostrid, 0) != 0 )
        BEGIN
            SELECT TOP 1 @freqLow = frequency_low,
                         @intLow = interval_low,
                         @dosLow = dosage_low,
                         @dosForm = dosage_form_amount_low,
                         @intUnit = interval_unit_low						 
              FROM fdb_med_dosage_order_def od (nolock)
             WHERE od.poeostrid = @poeostrid
        END

      IF @custom_dosage_id IS NOT NULL
         AND @custom_dosage_id <> '00000000-0000-0000-0000-000000000000'
        BEGIN
            SELECT TOP 1 @freqLow = frequency_low,
                         @intLow = interval_low,
                         @dosLow = dosage_low,
                         @dosForm = dosage_form_amount_low,
                         @intUnit = interval_unit_low
              FROM patient_medication pm (nolock)
                   LEFT OUTER JOIN fdb_med_dosage_order_custom oc (nolock)
                                ON pm.custom_dosage_order_id = oc.uniq_id
             WHERE pm.uniq_id = @uniq_id
               AND oc.uniq_id = @custom_dosage_id
        END

      SELECT @returnValue = CASE
                              WHEN ( @custom_dosage_id IS NULL
                                      OR @custom_dosage_id = '00000000-0000-0000-0000-000000000000' )
                                   AND ( Isnull(@poeostrid, 0) = 0 )
                              THEN 'Sig Missing'
                              WHEN Isnull(@freqLow, 0) = 0
                                    OR Isnull(@intLow, 0) = 0
                                    OR Isnull(@dosForm, 0) = 0
                                    OR Isnumeric(Isnull(@rx_quantity, 'Nope')) != 1
                              THEN 'Invalid Value'
							  WHEN @rx_quantity = '0'
							  THEN 'RX quantity is zero'
                              WHEN Isnull(@freqLow, 0) = 0
                                   AND Isnull(@intLow, 0) = 0
                                   AND Isnull(@dosForm, 0) = 0
                              THEN 'Custom'
                              ELSE Cast(Cast(Round(Cast(@rx_quantity AS NUMERIC(20, 10)) / ( @dosForm * @freqLow ) / ( CASE @intUnit
                                                                                                                         WHEN 'hour'
                                                                                                                         THEN 24 / @intLow
                                                                                                                         WHEN 'day'
                                                                                                                         THEN @intLow
                                                                                                                         WHEN 'weeks'
                                                                                                                         THEN @intLow / (7*@intLow)
                                                                                                                         WHEN 'week'
                                                                                                                         THEN @intLow / (7*@intLow)
                                                                                                                         ELSE @intLow
                                                                                                                       END ), 0) AS INTEGER) AS VARCHAR(50))
                            END
							
	  SET @returnValue = (CASE WHEN ISNUMERIC(@rxRefills) = 1 AND @rxRefills != 0 AND ISNUMERIC(@returnValue) = 1
	  						   THEN CAST(CAST(@returnValue AS NUMERIC(19,10)) * CAST(@rxRefills AS NUMERIC(19,10)) AS VARCHAR(75)) ELSE @returnValue END)

      RETURN @returnValue
  END
go
GRANT EXECUTE ON dbo.Chs_days_supply TO "public"
go