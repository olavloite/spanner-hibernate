package nl.topicus.hibernate.dialect;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.hibernate.boot.Metadata;
import org.hibernate.dialect.Dialect;
import org.hibernate.mapping.Index;
import org.hibernate.mapping.Table;
import org.hibernate.mapping.UniqueKey;
import org.hibernate.tool.schema.internal.StandardTableExporter;

public class CloudSpannerTableExporter extends StandardTableExporter
{

	public CloudSpannerTableExporter(Dialect dialect)
	{
		super(dialect);
	}

	@Override
	public String[] getSqlDropStrings(Table table, Metadata metadata)
	{
		Iterator<Index> indices = table.getIndexIterator();
		Iterator<UniqueKey> uniqueKeys = table.getUniqueKeyIterator();
		if (!indices.hasNext() && !uniqueKeys.hasNext())
			return super.getSqlDropStrings(table, metadata);

		List<String> dropIndices = new ArrayList<>();
		while (indices.hasNext())
			dropIndices.add("DROP INDEX `" + indices.next().getName() + "`");
		while (uniqueKeys.hasNext())
			dropIndices.add("DROP INDEX `" + uniqueKeys.next().getName() + "`");
		String[] tableDrop = super.getSqlDropStrings(table, metadata);

		String[] res = new String[dropIndices.size() + tableDrop.length];
		dropIndices.toArray(res);
		System.arraycopy(tableDrop, 0, res, dropIndices.size(), tableDrop.length);

		return res;
	}

}
