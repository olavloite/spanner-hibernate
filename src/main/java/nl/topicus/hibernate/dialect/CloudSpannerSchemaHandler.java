package nl.topicus.hibernate.dialect;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import org.hibernate.boot.Metadata;
import org.hibernate.boot.model.relational.AuxiliaryDatabaseObject;
import org.hibernate.boot.model.relational.Database;
import org.hibernate.dialect.Dialect;
import org.hibernate.tool.schema.spi.SchemaCreator;
import org.hibernate.tool.schema.spi.SchemaDropper;
import org.hibernate.tool.schema.spi.SchemaMigrator;

/**
 * This class intercepts calls to schema migrations/drops/creations and adds
 * Cloud Spanner statements that will automatically batch the DDL operations.
 * Batching DDL operations on Google Cloud Spanner is a lot faster than
 * executing them one by one. It is created as an {@link InvocationHandler}
 * instead of actual implementations of {@link SchemaDropper},
 * {@link SchemaCreator} and {@link SchemaMigrator} as these have some
 * differences between them in recent versions of Hibernate. This way, this
 * dialect can still be used with several different versions of Hibernate
 * without any changes.
 * 
 * @author loite
 *
 */
class CloudSpannerSchemaHandler implements InvocationHandler
{
	private static final String SET_AUTO_BATCH_EXPORT_IDENTIFIER = "SET_AUTO_BATCH_DDL_TRUE";

	private static final String EXECUTE_AND_RESET_AUTO_BATCH_EXPORT_IDENTIFIER = "EXECUTE_AND_RESET_AUTO_BATCH_DDL";

	private final class SetAutoBatch implements AuxiliaryDatabaseObject
	{
		private static final long serialVersionUID = 1L;

		@Override
		public String getExportIdentifier()
		{
			return SET_AUTO_BATCH_EXPORT_IDENTIFIER;
		}

		@Override
		public boolean appliesToDialect(Dialect dialect)
		{
			return CloudSpannerDialect.class.isAssignableFrom(dialect.getClass());
		}

		@Override
		public boolean beforeTablesOnCreation()
		{
			return !isMigration;
		}

		@Override
		public String[] sqlCreateStrings(Dialect dialect)
		{
			return new String[] { "SET_CONNECTION_PROPERTY AutoBatchDdlOperations=true" };
		}

		@Override
		public String[] sqlDropStrings(Dialect dialect)
		{
			return new String[] { "SET_CONNECTION_PROPERTY AutoBatchDdlOperations=true" };
		}

	}

	private final class ExecuteAndResetAutoBatch implements AuxiliaryDatabaseObject
	{
		private static final long serialVersionUID = 1L;

		@Override
		public String getExportIdentifier()
		{
			return EXECUTE_AND_RESET_AUTO_BATCH_EXPORT_IDENTIFIER;
		}

		@Override
		public boolean appliesToDialect(Dialect dialect)
		{
			return CloudSpannerDialect.class.isAssignableFrom(dialect.getClass());
		}

		@Override
		public boolean beforeTablesOnCreation()
		{
			return isMigration;
		}

		@Override
		public String[] sqlCreateStrings(Dialect dialect)
		{
			return new String[] { "EXECUTE_DDL_BATCH", "RESET_CONNECTION_PROPERTY AutoBatchDdlOperations" };
		}

		@Override
		public String[] sqlDropStrings(Dialect dialect)
		{
			return new String[] { "EXECUTE_DDL_BATCH", "RESET_CONNECTION_PROPERTY AutoBatchDdlOperations" };
		}

	}

	private final SetAutoBatch SET = new SetAutoBatch();

	private final ExecuteAndResetAutoBatch WAIT = new ExecuteAndResetAutoBatch();

	private final Object delegate;

	private final boolean isMigration;

	CloudSpannerSchemaHandler(Object delegate, boolean isMigration)
	{
		this.delegate = delegate;
		this.isMigration = isMigration;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
	{
		for (Object o : args)
		{
			if (o instanceof Metadata)
			{
				addAuxiliaryDatabaseObjects(((Metadata) o).getDatabase());
				break;
			}
		}
		return method.invoke(delegate, args);
	}

	private void addAuxiliaryDatabaseObjects(Database database)
	{
		// Adding multiple times is a no-op
		database.addAuxiliaryDatabaseObject(SET);
		database.addAuxiliaryDatabaseObject(WAIT);
	}

}
