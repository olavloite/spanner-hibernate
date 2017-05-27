package nl.topicus.hibernate.dialect;

import org.hibernate.boot.Metadata;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.unique.DefaultUniqueDelegate;
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
	protected String getDropUnique()
	{
		return " drop index ";
	}

}
