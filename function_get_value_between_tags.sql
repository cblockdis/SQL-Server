ALTER FUNCTION dbo.Parse_value(@string       VARCHAR(MAX),
                               @startTag     VARCHAR(75),
                               @endTag       VARCHAR(75),
                               @removeSpaces BIT = 0,
                               @startPos     INT = 1)
RETURNS VARCHAR(8000)
  BEGIN
      IF @string LIKE ( '%' + @startTag + '%' + Isnull(@endTag,'') + '%' )
        BEGIN
            DECLARE @returnValue VARCHAR(MAX) = NULL
            DECLARE @start INT = Charindex(@startTag, @string, @startPos)
            DECLARE @end INT = -1

            IF @endTag IS NULL
              BEGIN
                  SET @end = Len(@string) + 1
              END
            ELSE
              BEGIN
                  SET @end = Charindex(@endTag, @string, @start + 1)
              END

            DECLARE @length INT = @end - @start

            SELECT @returnValue = Substring(@string, @start, @length)

            SELECT @returnValue = Replace(@returnValue, @startTag, '')

            SELECT @returnValue = dbo.Chs_trim(@returnValue)

            IF @removeSpaces = 1
              BEGIN
                  SELECT @returnValue = Replace(@returnValue, ' ', '')
              END

            RETURN @returnValue
        END

      RETURN NULL
  END
go