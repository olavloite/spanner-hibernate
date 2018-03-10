package nl.topicus.hibernate.dialect;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.hibernate.boot.Metadata;
import org.hibernate.mapping.Table;
import org.hibernate.tool.schema.internal.StandardTableExporter;

public class CloudSpannerTableExporter extends StandardTableExporter
{
	private final CloudSpannerDialect dialect;

	public CloudSpannerTableExporter(CloudSpannerDialect dialect)
	{
		super(dialect);
		this.dialect = dialect;
	}

	@Override
	public String[] getSqlDropStrings(Table table, Metadata metadata)
	{
		// Check for actually existing table and indices.
		if (!tableExists(table))
			return new String[] {};
		Set<String> existingIndices = getIndicesExcludingPK(table);

		if (existingIndices.isEmpty())
			return super.getSqlDropStrings(table, metadata);

		List<String> dropIndices = new ArrayList<>();
		for (String index : existingIndices)
		{
			dropIndices.add("DROP INDEX `" + index + "`");
		}
		String[] tableDrop = super.getSqlDropStrings(table, metadata);

		String[] res = new String[dropIndices.size() + tableDrop.length];
		dropIndices.toArray(res);
		System.arraycopy(tableDrop, 0, res, dropIndices.size(), tableDrop.length);

		return res;
	}

	private boolean tableExists(Table table)
	{
		if (dialect.getMetadata() == null)
			return false;
		boolean exists = true;
		try (ResultSet tables = dialect.getMetadata().getTables(table.getCatalog(), table.getSchema(), table.getName(),
				null))
		{
			exists = tables.next();
		}
		catch (SQLException e)
		{
			// ignore at this point, just try to drop it.
		}
		return exists;
	}

	private Set<String> getIndicesExcludingPK(Table table)
	{
		Set<String> res = new HashSet<>();
		if (dialect.getMetadata() == null)
			return res;
		try (ResultSet indices = dialect.getMetadata().getIndexInfo(table.getCatalog(), table.getSchema(),
				table.getName(), false, false))
		{
			while (indices.next())
			{
				if (!indices.getString("INDEX_NAME").equalsIgnoreCase("PRIMARY_KEY"))
					res.add(indices.getString("INDEX_NAME"));
			}
		}
		catch (SQLException e)
		{
			// ignore at this point, just return an empty set
		}
		return res;
	}

}
