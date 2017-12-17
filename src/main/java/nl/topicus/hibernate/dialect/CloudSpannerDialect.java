package nl.topicus.hibernate.dialect;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;

import org.hibernate.boot.Metadata;
import org.hibernate.dialect.Dialect;
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
