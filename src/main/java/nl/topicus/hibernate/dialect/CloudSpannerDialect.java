package nl.topicus.hibernate.dialect;

import java.sql.Types;

import org.hibernate.dialect.Dialect;

/**
 * Hibernate SQL dialect for Google Cloud Spanner
 * 
 * @author loite
 *
 */
public class CloudSpannerDialect extends Dialect
{

	public CloudSpannerDialect()
	{
		registerColumnType(Types.BOOLEAN, "BOOL");
		registerColumnType(Types.BIT, "BOOL");
		registerColumnType(Types.BIGINT, "INT64");
		registerColumnType(Types.SMALLINT, "INT64");
		registerColumnType(Types.TINYINT, "INT64");
		registerColumnType(Types.INTEGER, "INT64");
		registerColumnType(Types.CHAR, "STRING(1)");
		registerColumnType(Types.VARCHAR, "STRING($l)");
		registerColumnType(Types.FLOAT, "FLOAT64");
		registerColumnType(Types.DOUBLE, "FLOAT64");
		registerColumnType(Types.DECIMAL, "FLOAT64");
		registerColumnType(Types.DATE, "DATE");
		registerColumnType(Types.TIME, "TIMESTAMP");
		registerColumnType(Types.TIMESTAMP, "TIMESTAMP");
		registerColumnType(Types.VARBINARY, "BYTES($1)");
		registerColumnType(Types.BINARY, "BYTES($1)");
		registerColumnType(Types.LONGVARCHAR, "STRING($1)");
		registerColumnType(Types.LONGVARBINARY, "BYTES($1)");
		registerColumnType(Types.CLOB, "STRING($1)");
		registerColumnType(Types.BLOB, "BYTES($1)");
		registerColumnType(Types.NUMERIC, "FLOAT64");
	}

	@Override
	public String getAddColumnString()
	{
		return "add column";
	}

	@Override
	public String toBooleanValueString(boolean bool)
	{
		return bool ? "true" : "false";
	}

}
