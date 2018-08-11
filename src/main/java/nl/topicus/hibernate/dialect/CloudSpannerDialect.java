package nl.topicus.hibernate.dialect;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;

import org.hibernate.boot.Metadata;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.function.StandardSQLFunction;
import org.hibernate.dialect.pagination.AbstractLimitHandler;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.dialect.pagination.LimitHelper;
import org.hibernate.dialect.unique.UniqueDelegate;
import org.hibernate.engine.jdbc.env.spi.IdentifierHelper;
import org.hibernate.engine.jdbc.env.spi.IdentifierHelperBuilder;
import org.hibernate.engine.spi.RowSelection;
import org.hibernate.mapping.ForeignKey;
import org.hibernate.mapping.Table;
import org.hibernate.tool.schema.internal.StandardTableExporter;
import org.hibernate.tool.schema.spi.Exporter;
import org.hibernate.type.StandardBasicTypes;

/**
 * Hibernate SQL dialect for Google Cloud Spanner
 * 
 * @author loite
 *
 */
public class CloudSpannerDialect extends Dialect
{
	private static final AbstractLimitHandler LIMIT_HANDLER = new AbstractLimitHandler()
	{
		@Override
		public String processSql(String sql, RowSelection selection)
		{
			final boolean hasOffset = LimitHelper.hasFirstRow(selection);
			return sql + (hasOffset ? " limit ? offset ?" : " limit ?");
		}

		@Override
		public boolean supportsLimit()
		{
			return true;
		}

		@Override
		public boolean bindLimitParametersInReverseOrder()
		{
			return true;
		}
	};

	private final UniqueDelegate uniqueDelegate;

	private DatabaseMetaData metadata;

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
		registerColumnType(Types.VARBINARY, "BYTES($l)");
		registerColumnType(Types.BINARY, "BYTES($l)");
		registerColumnType(Types.LONGVARCHAR, "STRING($l)");
		registerColumnType(Types.LONGVARBINARY, "BYTES($l)");
		registerColumnType(Types.CLOB, "STRING($l)");
		registerColumnType(Types.BLOB, "BYTES($l)");
		registerColumnType(Types.NUMERIC, "FLOAT64");

		registerFunction("concat", new StandardSQLFunction("concat", StandardBasicTypes.NSTRING));
		registerFunction("string_agg", new StandardSQLFunction("string_agg", StandardBasicTypes.NSTRING));
		registerFunction("FARM_FINGERPRINT",
				new StandardSQLFunction("FARM_FINGERPRINT", StandardBasicTypes.BIG_INTEGER));
		registerFunction("SHA1", new StandardSQLFunction("SHA1", StandardBasicTypes.BINARY));
		registerFunction("SHA256", new StandardSQLFunction("SHA256", StandardBasicTypes.BINARY));
		registerFunction("SHA512", new StandardSQLFunction("SHA512", StandardBasicTypes.BINARY));
		registerFunction("BYTE_LENGTH", new StandardSQLFunction("BYTE_LENGTH", StandardBasicTypes.BIG_INTEGER));
		registerFunction("CHAR_LENGTH", new StandardSQLFunction("CHAR_LENGTH", StandardBasicTypes.BIG_INTEGER));
		registerFunction("CHARACTER_LENGTH",
				new StandardSQLFunction("CHARACTER_LENGTH", StandardBasicTypes.BIG_INTEGER));
		registerFunction("CODE_POINTS_TO_BYTES",
				new StandardSQLFunction("CODE_POINTS_TO_BYTES", StandardBasicTypes.BINARY));
		registerFunction("CODE_POINTS_TO_STRING",
				new StandardSQLFunction("CODE_POINTS_TO_STRING", StandardBasicTypes.NSTRING));
		registerFunction("ENDS_WITH", new StandardSQLFunction("ENDS_WITH", StandardBasicTypes.BOOLEAN));
		registerFunction("FORMAT", new StandardSQLFunction("FORMAT", StandardBasicTypes.NSTRING));
		registerFunction("FROM_BASE64", new StandardSQLFunction("FROM_BASE64", StandardBasicTypes.BINARY));
		registerFunction("FROM_HEX", new StandardSQLFunction("FROM_HEX", StandardBasicTypes.BINARY));
		registerFunction("LENGTH", new StandardSQLFunction("LENGTH", StandardBasicTypes.BIG_INTEGER));
		registerFunction("LPAD", new StandardSQLFunction("LPAD", StandardBasicTypes.NSTRING));
		registerFunction("LOWER", new StandardSQLFunction("LOWER", StandardBasicTypes.NSTRING));
		registerFunction("LTRIM", new StandardSQLFunction("LTRIM", StandardBasicTypes.NSTRING));
		registerFunction("REGEXP_CONTAINS", new StandardSQLFunction("REGEXP_CONTAINS", StandardBasicTypes.BOOLEAN));
		registerFunction("REGEXP_EXTRACT", new StandardSQLFunction("REGEXP_EXTRACT", StandardBasicTypes.NSTRING));
		registerFunction("REGEXP_EXTRACT_ALL",
				new StandardSQLFunction("REGEXP_EXTRACT_ALL", StandardBasicTypes.NSTRING));
		registerFunction("REGEXP_REPLACE", new StandardSQLFunction("REGEXP_REPLACE", StandardBasicTypes.NSTRING));
		registerFunction("REPLACE", new StandardSQLFunction("REPLACE", StandardBasicTypes.NSTRING));
		registerFunction("REPEAT", new StandardSQLFunction("REPEAT", StandardBasicTypes.NSTRING));
		registerFunction("REVERSE", new StandardSQLFunction("REVERSE", StandardBasicTypes.NSTRING));
		registerFunction("RPAD", new StandardSQLFunction("RPAD", StandardBasicTypes.NSTRING));
		registerFunction("RTRIM", new StandardSQLFunction("RTRIM", StandardBasicTypes.NSTRING));
		registerFunction("SAFE_CONVERT_BYTES_TO_STRING",
				new StandardSQLFunction("SAFE_CONVERT_BYTES_TO_STRING", StandardBasicTypes.NSTRING));
		registerFunction("SPLIT", new StandardSQLFunction("SPLIT", StandardBasicTypes.NSTRING));
		registerFunction("STARTS_WITH", new StandardSQLFunction("STARTS_WITH", StandardBasicTypes.BOOLEAN));
		registerFunction("STRPOS", new StandardSQLFunction("STRPOS", StandardBasicTypes.BIG_INTEGER));
		registerFunction("SUBSTR", new StandardSQLFunction("SUBSTR", StandardBasicTypes.NSTRING));
		registerFunction("TO_BASE64", new StandardSQLFunction("TO_BASE64", StandardBasicTypes.NSTRING));
		registerFunction("TO_CODE_POINTS", new StandardSQLFunction("TO_CODE_POINTS", StandardBasicTypes.BIG_INTEGER));
		registerFunction("TO_HEX", new StandardSQLFunction("TO_HEX", StandardBasicTypes.NSTRING));
		registerFunction("TRIM", new StandardSQLFunction("TRIM", StandardBasicTypes.NSTRING));
		registerFunction("UPPER", new StandardSQLFunction("UPPER", StandardBasicTypes.NSTRING));
		registerFunction("JSON_QUERY", new StandardSQLFunction("JSON_QUERY", StandardBasicTypes.NSTRING));
		registerFunction("JSON_VALUE", new StandardSQLFunction("JSON_VALUE", StandardBasicTypes.NSTRING));

		registerFunction("ARRAY_LENGTH", new StandardSQLFunction("ARRAY_LENGTH", StandardBasicTypes.BIG_INTEGER));
		registerFunction("ARRAY_TO_STRING", new StandardSQLFunction("ARRAY_TO_STRING", StandardBasicTypes.NSTRING));
		registerFunction("GENERATE_ARRAY", new StandardSQLFunction("GENERATE_ARRAY", StandardBasicTypes.BIG_INTEGER));
		registerFunction("GENERATE_DATE_ARRAY",
				new StandardSQLFunction("GENERATE_DATE_ARRAY", StandardBasicTypes.DATE));

		registerFunction("CURRENT_DATE", new StandardSQLFunction("CURRENT_DATE", StandardBasicTypes.DATE));
		registerFunction("EXTRACT", new StandardSQLFunction("EXTRACT", StandardBasicTypes.BIG_INTEGER));
		registerFunction("DATE", new StandardSQLFunction("DATE", StandardBasicTypes.DATE));
		registerFunction("DATE_ADD", new StandardSQLFunction("DATE_ADD", StandardBasicTypes.DATE));
		registerFunction("DATE_SUB", new StandardSQLFunction("DATE_SUB", StandardBasicTypes.DATE));
		registerFunction("DATE_DIFF", new StandardSQLFunction("DATE_DIFF", StandardBasicTypes.BIG_INTEGER));
		registerFunction("DATE_TRUNC", new StandardSQLFunction("DATE_TRUNC", StandardBasicTypes.DATE));
		registerFunction("DATE_FROM_UNIX_DATE",
				new StandardSQLFunction("DATE_FROM_UNIX_DATE", StandardBasicTypes.DATE));
		registerFunction("FORMAT_DATE", new StandardSQLFunction("FORMAT_DATE", StandardBasicTypes.NSTRING));
		registerFunction("PARSE_DATE", new StandardSQLFunction("PARSE_DATE", StandardBasicTypes.DATE));
		registerFunction("UNIX_DATE", new StandardSQLFunction("UNIX_DATE", StandardBasicTypes.BIG_INTEGER));

		registerFunction("CURRENT_TIMESTAMP",
				new StandardSQLFunction("CURRENT_TIMESTAMP", StandardBasicTypes.TIMESTAMP));
		registerFunction("STRING", new StandardSQLFunction("STRING", StandardBasicTypes.NSTRING));
		registerFunction("TIMESTAMP", new StandardSQLFunction("TIMESTAMP", StandardBasicTypes.TIMESTAMP));
		registerFunction("TIMESTAMP_ADD", new StandardSQLFunction("TIMESTAMP_ADD", StandardBasicTypes.TIMESTAMP));
		registerFunction("TIMESTAMP_SUB", new StandardSQLFunction("TIMESTAMP_SUB", StandardBasicTypes.TIMESTAMP));
		registerFunction("TIMESTAMP_DIFF", new StandardSQLFunction("TIMESTAMP_DIFF", StandardBasicTypes.BIG_INTEGER));
		registerFunction("TIMESTAMP_TRUNC", new StandardSQLFunction("TIMESTAMP_TRUNC", StandardBasicTypes.TIMESTAMP));
		registerFunction("FORMAT_TIMESTAMP", new StandardSQLFunction("FORMAT_TIMESTAMP", StandardBasicTypes.STRING));
		registerFunction("PARSE_TIMESTAMP", new StandardSQLFunction("PARSE_TIMESTAMP", StandardBasicTypes.TIMESTAMP));
		registerFunction("TIMESTAMP_SECONDS",
				new StandardSQLFunction("TIMESTAMP_SECONDS", StandardBasicTypes.TIMESTAMP));
		registerFunction("TIMESTAMP_MILLIS", new StandardSQLFunction("TIMESTAMP_MILLIS", StandardBasicTypes.TIMESTAMP));
		registerFunction("TIMESTAMP_MICROS", new StandardSQLFunction("TIMESTAMP_MICROS", StandardBasicTypes.TIMESTAMP));
		registerFunction("UNIX_SECONDS", new StandardSQLFunction("UNIX_SECONDS", StandardBasicTypes.BIG_INTEGER));
		registerFunction("UNIX_MILLIS", new StandardSQLFunction("UNIX_MILLIS", StandardBasicTypes.BIG_INTEGER));
		registerFunction("UNIX_MICROS", new StandardSQLFunction("UNIX_MICROS", StandardBasicTypes.BIG_INTEGER));
		registerFunction("PARSE_TIMESTAMP", new StandardSQLFunction("PARSE_TIMESTAMP", StandardBasicTypes.TIMESTAMP));

		registerFunction("bit_and", new StandardSQLFunction("bit_and", StandardBasicTypes.BIG_INTEGER));
		registerFunction("bit_or", new StandardSQLFunction("bit_or", StandardBasicTypes.BIG_INTEGER));
		registerFunction("bit_xor", new StandardSQLFunction("bit_xor", StandardBasicTypes.BIG_INTEGER));
		registerFunction("logical_and", new StandardSQLFunction("logical_and", StandardBasicTypes.BOOLEAN));
		registerFunction("logical_or", new StandardSQLFunction("logical_or", StandardBasicTypes.BOOLEAN));

		registerFunction("is_inf", new StandardSQLFunction("is_inf", StandardBasicTypes.BOOLEAN));
		registerFunction("is_nan", new StandardSQLFunction("is_nan", StandardBasicTypes.BOOLEAN));

		registerFunction("IEEE_DIVIDE", new StandardSQLFunction("IEEE_DIVIDE", StandardBasicTypes.DOUBLE));
		registerFunction("SQRT", new StandardSQLFunction("SQRT", StandardBasicTypes.DOUBLE));
		registerFunction("POW", new StandardSQLFunction("POW", StandardBasicTypes.DOUBLE));
		registerFunction("POWER", new StandardSQLFunction("POWER", StandardBasicTypes.DOUBLE));
		registerFunction("EXP", new StandardSQLFunction("EXP", StandardBasicTypes.DOUBLE));
		registerFunction("LN", new StandardSQLFunction("LN", StandardBasicTypes.DOUBLE));
		registerFunction("LOG", new StandardSQLFunction("LOG", StandardBasicTypes.DOUBLE));
		registerFunction("LOG10", new StandardSQLFunction("LOG10", StandardBasicTypes.DOUBLE));
		registerFunction("GREATEST", new StandardSQLFunction("GREATEST", StandardBasicTypes.DOUBLE));
		registerFunction("LEAST", new StandardSQLFunction("LEAST", StandardBasicTypes.DOUBLE));
		registerFunction("DIV", new StandardSQLFunction("DIV", StandardBasicTypes.DOUBLE));
		registerFunction("MOD", new StandardSQLFunction("MOD", StandardBasicTypes.DOUBLE));
		registerFunction("ROUND", new StandardSQLFunction("ROUND", StandardBasicTypes.DOUBLE));
		registerFunction("TRUNC", new StandardSQLFunction("TRUNC", StandardBasicTypes.DOUBLE));
		registerFunction("CEIL", new StandardSQLFunction("CEIL", StandardBasicTypes.DOUBLE));
		registerFunction("CEILING", new StandardSQLFunction("CEILING", StandardBasicTypes.DOUBLE));
		registerFunction("FLOOR", new StandardSQLFunction("FLOOR", StandardBasicTypes.DOUBLE));
		registerFunction("COS", new StandardSQLFunction("COS", StandardBasicTypes.DOUBLE));
		registerFunction("COSH", new StandardSQLFunction("COSH", StandardBasicTypes.DOUBLE));
		registerFunction("ACOS", new StandardSQLFunction("ACOS", StandardBasicTypes.DOUBLE));
		registerFunction("ACOSH", new StandardSQLFunction("ACOSH", StandardBasicTypes.DOUBLE));
		registerFunction("SIN", new StandardSQLFunction("SIN", StandardBasicTypes.DOUBLE));
		registerFunction("SINH", new StandardSQLFunction("SINH", StandardBasicTypes.DOUBLE));
		registerFunction("ASIN", new StandardSQLFunction("ASIN", StandardBasicTypes.DOUBLE));
		registerFunction("ASINH", new StandardSQLFunction("ASINH", StandardBasicTypes.DOUBLE));
		registerFunction("TAN", new StandardSQLFunction("TAN", StandardBasicTypes.DOUBLE));
		registerFunction("TANH", new StandardSQLFunction("TANH", StandardBasicTypes.DOUBLE));
		registerFunction("ATAN", new StandardSQLFunction("ATAN", StandardBasicTypes.DOUBLE));
		registerFunction("ATANH", new StandardSQLFunction("ATANH", StandardBasicTypes.DOUBLE));
		registerFunction("ATAN2", new StandardSQLFunction("ATAN2", StandardBasicTypes.DOUBLE));

		uniqueDelegate = new CloudSpannerUniqueDelegate(this);
	}

	@Override
	public IdentifierHelper buildIdentifierHelper(IdentifierHelperBuilder builder, DatabaseMetaData dbMetaData)
			throws SQLException
	{
		// only override this method in order to be able to access the database
		// metadata
		this.metadata = dbMetaData;
		return super.buildIdentifierHelper(builder, dbMetaData);
	}

	@Override
	public UniqueDelegate getUniqueDelegate()
	{
		return uniqueDelegate;
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

	private static final class EmptyForeignKeyExporter implements Exporter<ForeignKey>
	{

		@Override
		public String[] getSqlCreateStrings(ForeignKey exportable, Metadata metadata)
		{
			return NO_COMMANDS;
		}

		@Override
		public String[] getSqlDropStrings(ForeignKey exportable, Metadata metadata)
		{
			return NO_COMMANDS;
		}

	}

	private EmptyForeignKeyExporter foreignKeyExporter = new EmptyForeignKeyExporter();

	@Override
	public Exporter<ForeignKey> getForeignKeyExporter()
	{
		return foreignKeyExporter;
	}

	private StandardTableExporter tableExporter = new CloudSpannerTableExporter(this);

	@Override
	public Exporter<Table> getTableExporter()
	{
		return tableExporter;
	}

	@Override
	public boolean canCreateSchema()
	{
		return false;
	}

	@Override
	public LimitHandler getLimitHandler()
	{
		return LIMIT_HANDLER;
	}

	@Override
	public String getLimitString(String sql, boolean hasOffset)
	{
		return sql + (hasOffset ? " limit ? offset ?" : " limit ?");
	}

	@Override
	public boolean bindLimitParametersInReverseOrder()
	{
		return true;
	}

	@Override
	public boolean supportsUnionAll()
	{
		return true;
	}

	DatabaseMetaData getMetadata()
	{
		return metadata;
	}

}
