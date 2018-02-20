ALTER FUNCTION [dbo].[Chs_percent] (@numerator   VARCHAR(75),
                                    @denominator VARCHAR(75),
                                    @show_symbol BIT = 0)
RETURNS VARCHAR(75)
AS
  BEGIN
      /* 
      	Takes the two values provided and returns the percentage value for them. When 
        show_symbol is set to 1 [TRUE], include the '%' symbol. Value returns as #.##% (ie. 31.48%)
      */
      DECLARE @returnValue VARCHAR(75) = NULL

      /* Make sure the values provided are actually numbers */
      IF Isnumeric(@numerator) = 1
         AND Isnumeric(@denominator) = 1
        BEGIN
            DECLARE @num NUMERIC(16, 10) = Cast(@numerator AS NUMERIC(16, 10))
            DECLARE @den NUMERIC(16, 10) = Cast(@denominator AS NUMERIC(16, 10))

            IF @den > 0
              BEGIN
                  DECLARE @perc NUMERIC(16, 10) = @num / @den * 100

                  SET @returnValue = Cast(CONVERT(NUMERIC(6, 2), @perc) AS VARCHAR(75)) + ( CASE
                                                                                              WHEN @show_symbol = 1
                                                                                              THEN '%'
                                                                                              ELSE ''
                                                                                            END )
              END
            ELSE
              BEGIN
                  SET @returnValue = NULL
              END
        END

      RETURN @returnValue
  END
go
GRANT EXECUTE ON dbo.Chs_percent TO "public"
go