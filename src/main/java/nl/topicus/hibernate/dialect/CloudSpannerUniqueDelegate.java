package nl.topicus.hibernate.dialect;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.hibernate.boot.Metadata;
import org.hibernate.dialect.unique.DefaultUniqueDelegate;
import org.hibernate.engine.config.spi.ConfigurationService;
import org.hibernate.engine.config.spi.StandardConverters;
import org.hibernate.mapping.UniqueKey;

public class CloudSpannerUniqueDelegate extends DefaultUniqueDelegate
{
	private static final class UniqueIndex
	{
		private String name;

		private String table;

		private Set<String> columns = new HashSet<>();

		private UniqueIndex(String name, String table, String column)
		{
			this.name = name;
			this.table = table;
			this.columns.add(column);
		}

		@Override
		public boolean equals(Object other)
		{
			if (!(other instanceof UniqueIndex))
				return false;
			return ((UniqueIndex) other).name.equals(name);
		}

		@Override
		public int hashCode()
		{
			return name.hashCode();
		}
	}

	private static final class UniqueIndices
	{
		private Map<String, UniqueIndex> map = new HashMap<>();

		public void addIndexedColumn(String indexName, String table, String column)
		{
			String key = table + "." + indexName;
			UniqueIndex idx = map.get(key);
			if (idx == null)
			{
				idx = new UniqueIndex(indexName, table, column.toUpperCase());
				map.put(key, idx);
			}
			else
			{
				idx.columns.add(column.toUpperCase());
			}
		}

		public UniqueIndex getIndex(UniqueKey uniqueKey)
		{
			for (UniqueIndex idx : map.values())
			{
				if (idx.table.equalsIgnoreCase(uniqueKey.getTable().getName()))
				{
					List<String> cols = uniqueKey.getColumns().stream().map(x -> x.getName().toUpperCase())
							.collect(Collectors.toList());
					if (idx.columns.containsAll(cols) && cols.containsAll(idx.columns))
					{
						return idx;
					}
				}
			}
			return null;
		}

		public void removeIndex(UniqueKey uniqueKey)
		{
			String key = uniqueKey.getTable().getName() + "." + uniqueKey.getName();
			map.remove(key);
		}
	}

	private UniqueIndices indices;

	public CloudSpannerUniqueDelegate(CloudSpannerDialect dialect)
	{
		super(dialect);
	}

	private void initIndices()
	{
		if (indices == null)
		{
			DatabaseMetaData dbMetadata = ((CloudSpannerDialect) this.dialect).getMetadata();
			if (dbMetadata != null)
			{
				indices = new UniqueIndices();
				try (ResultSet rs = dbMetadata.getIndexInfo("", "", null, true, false))
				{
					while (rs.next())
					{
						String indexName = rs.getString("INDEX_NAME");
						String tableName = rs.getString("TABLE_NAME");
						String column = rs.getString("COLUMN_NAME");
						indices.addIndexedColumn(indexName, tableName, column);
					}
				}
				catch (SQLException e)
				{
					// unable to access index info
				}
			}
		}
	}

	@Override
	public String getAlterTableToAddUniqueKeyCommand(UniqueKey uniqueKey, Metadata metadata)
	{
		ConfigurationService config = metadata.getDatabase().getServiceRegistry()
				.getService(ConfigurationService.class);
		if (config != null)
		{
			String value = config.getSetting("hibernate.hbm2ddl.auto", StandardConverters.STRING);
			if (!value.equalsIgnoreCase("update"))
			{
				// We should only check whether it is already present in an
				// update scenario, in all other scenarios, just return the
				// actual create statement.
				return org.hibernate.mapping.Index.buildSqlCreateIndexString(dialect, uniqueKey.getName(),
						uniqueKey.getTable(), uniqueKey.columnIterator(), uniqueKey.getColumnOrderMap(), true,
						metadata);
			}
		}
		// First check that this unique key is not already present, as this is a
		// lot faster than trying to create it and then fail.
		initIndices();
		UniqueIndex idx = indices.getIndex(uniqueKey);
		if (idx != null)
		{
			return null;
		}
		return org.hibernate.mapping.Index.buildSqlCreateIndexString(dialect, uniqueKey.getName(), uniqueKey.getTable(),
				uniqueKey.columnIterator(), uniqueKey.getColumnOrderMap(), true, metadata);
	}

	@Override
	public String getAlterTableToDropUniqueKeyCommand(UniqueKey uniqueKey, Metadata metadata)
	{
		// First check that this unique key actually is present, as this is a
		// lot faster than trying to drop it and then fail.
		initIndices();
		UniqueIndex idx = indices.getIndex(uniqueKey);
		if (idx == null)
		{
			return null;
		}
		// Remove from cache
		indices.removeIndex(uniqueKey);
		final StringBuilder buf = new StringBuilder("DROP INDEX ");
		buf.append(dialect.quote(uniqueKey.getName()));

		return buf.toString();
	}

}
