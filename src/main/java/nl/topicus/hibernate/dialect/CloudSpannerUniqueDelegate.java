package nl.topicus.hibernate.dialect;

import org.hibernate.boot.Metadata;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.unique.DefaultUniqueDelegate;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.mapping.UniqueKey;

public class CloudSpannerUniqueDelegate extends DefaultUniqueDelegate
{
	public CloudSpannerUniqueDelegate(Dialect dialect)
	{
		super(dialect);
	}

	@Override
	public String getAlterTableToAddUniqueKeyCommand(UniqueKey uniqueKey, Metadata metadata)
	{
		return org.hibernate.mapping.Index.buildSqlCreateIndexString(dialect, uniqueKey.getName(),
				uniqueKey.getTable(), uniqueKey.columnIterator(), uniqueKey.getColumnOrderMap(), true, metadata);
	}

	@Override
	public String getAlterTableToDropUniqueKeyCommand(UniqueKey uniqueKey, Metadata metadata)
	{
		final JdbcEnvironment jdbcEnvironment = metadata.getDatabase().getJdbcEnvironment();

		final String tableName = jdbcEnvironment.getQualifiedObjectNameFormatter().format(
				uniqueKey.getTable().getQualifiedTableName(), dialect);

		final StringBuilder buf = new StringBuilder("alter table ");
		buf.append(tableName);
		buf.append(" drop index ");
		buf.append(dialect.quote(uniqueKey.getName()));
		return buf.toString();
	}

}
