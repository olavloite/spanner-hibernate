package nl.topicus.hibernate.dialect;

import java.lang.reflect.Proxy;
import java.util.Map;

import org.hibernate.tool.schema.internal.HibernateSchemaManagementTool;
import org.hibernate.tool.schema.spi.SchemaCreator;
import org.hibernate.tool.schema.spi.SchemaDropper;
import org.hibernate.tool.schema.spi.SchemaMigrator;

/**
 * SchemaManagementTool specifically for Google Cloud Spanner. This tool can
 * make use of the automatic batching of DDL statements for Google Cloud Spanner
 * 
 * @author loite
 *
 */
public class CloudSpannerSchemaManagementTool extends HibernateSchemaManagementTool
{
	@Override
	public SchemaCreator getSchemaCreator(@SuppressWarnings("rawtypes") Map options)
	{
		CloudSpannerSchemaHandler handler = new CloudSpannerSchemaHandler(super.getSchemaCreator(options), false);
		return (SchemaCreator) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[] { SchemaCreator.class },
				handler);
	}

	@Override
	public SchemaMigrator getSchemaMigrator(@SuppressWarnings("rawtypes") Map options)
	{
		CloudSpannerSchemaHandler handler = new CloudSpannerSchemaHandler(super.getSchemaMigrator(options), true);
		return (SchemaMigrator) Proxy.newProxyInstance(getClass().getClassLoader(),
				new Class[] { SchemaMigrator.class }, handler);
	}

	private static final long serialVersionUID = 1L;

	@Override
	public SchemaDropper getSchemaDropper(@SuppressWarnings("rawtypes") Map options)
	{
		CloudSpannerSchemaHandler handler = new CloudSpannerSchemaHandler(super.getSchemaDropper(options), false);
		return (SchemaDropper) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[] { SchemaDropper.class },
				handler);
	}

}
