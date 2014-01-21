package org.normandra.cassandra;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.NullArgumentException;
import org.normandra.DatabaseConstruction;
import org.normandra.NormandraDatabase;
import org.normandra.NormandraException;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.DatabaseMeta;
import org.normandra.meta.EntityMeta;
import org.normandra.data.ColumnAccessor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * a cassandra database
 * <p/>
 * User: bowen
 * Date: 8/31/13
 */
public class CassandraDatabase implements NormandraDatabase
{
    public static final String KEYSPACE = "cassandra.keyspace";

    public static final String HOSTS = "cassandra.hosts";

    public static final String PORT = "cassandra.hosts";

    private final Cluster cluster;

    private final String keyspaceName;

    private final DatabaseConstruction constructionMode;

    private Session session;


    public CassandraDatabase(final String keyspace, final Cluster cluster, final DatabaseConstruction mode)
    {
        if (null == keyspace)
        {
            throw new NullArgumentException("keyspace");
        }
        if (null == cluster)
        {
            throw new NullArgumentException("host");
        }
        if (null == mode)
        {
            throw new NullArgumentException("mode");
        }
        this.keyspaceName = keyspace;
        this.cluster = cluster;
        this.constructionMode = mode;
    }


    @Override
    public <T> void save(final EntityMeta meta, final T element) throws NormandraException
    {
        if (null == meta)
        {
            throw new NullArgumentException("entity metadata");
        }
        if (null == element)
        {
            throw new NullArgumentException("element");
        }

        try
        {
            boolean hasValue = false;
            Insert statement = QueryBuilder.insertInto(this.keyspaceName, meta.getTable());
            for (final ColumnMeta column : meta.getColumns())
            {
                final ColumnAccessor<?> accessor = column.getAccessor();
                final Object value = accessor.getValue(element);
                if (value != null)
                {
                    statement = statement.value(column.getName(), value);
                    hasValue = true;
                }
            }
            if (!hasValue)
            {
                throw new NormandraException("No column values found - cannot save empty entity.");
            }
            this.ensureSession().execute(statement);
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to save entity [" + meta + "] instance [" + element + "].", e);
        }
    }


    @Override
    public void refresh(final DatabaseMeta meta) throws NormandraException
    {
        if (null == meta)
        {
            throw new NullArgumentException("database metadata");
        }

        if (DatabaseConstruction.NONE.equals(this.constructionMode))
        {
            return;
        }

        for (final Map.Entry<String, Collection<EntityMeta>> entry : meta.getEntities().entrySet())
        {
            final String table = entry.getKey();
            final List<Statement> statements = new ArrayList<>();

            // drop table as required
            if (DatabaseConstruction.RECREATE.equals(this.constructionMode) && this.hasTable(table))
            {
                final StringBuilder cql = new StringBuilder();
                cql.append("DROP TABLE ").append(table).append(";");
                statements.add(new SimpleStatement(cql.toString()));
            }

            // create table schema
            final Set<ColumnMeta> uniqueSet = new TreeSet<>();
            final Collection<ColumnMeta> primaryColumns = new ArrayList<>();
            final Collection<ColumnMeta> allColumns = new ArrayList<>();
            for (final EntityMeta entity : entry.getValue())
            {
                for (final ColumnMeta column : entity)
                {
                    if (!uniqueSet.contains(column))
                    {
                        uniqueSet.add(column);
                        allColumns.add(column);
                        if (column.isPrimaryKey())
                        {
                            primaryColumns.add(column);
                        }
                    }
                }
            }
            if (DatabaseConstruction.UPDATE.equals(this.constructionMode))
            {
                // ensure we create base database with all keys - then update/add columns in separate comment
                statements.add(defineTable(table, primaryColumns));
                for (final ColumnMeta column : allColumns)
                {
                    final String name = column.getName();
                    if (!column.isPrimaryKey() && !this.hasColumn(table, name))
                    {
                        final StringBuilder cql = new StringBuilder();
                        final String type = CassandraUtils.columnType(column);
                        cql.append("ALTER TABLE ").append(table).append(IOUtils.LINE_SEPARATOR);
                        cql.append("ADD ").append(name).append(" ").append(type).append(";");
                        statements.add(new SimpleStatement(cql.toString()));
                    }
                }
            }
            else
            {
                // create table and column definitions in one command
                statements.add(defineTable(table, allColumns));
            }

            // execute batch statements
            if (!statements.isEmpty())
            {
                final Statement[] list = statements.toArray(new Statement[statements.size()]);
                for (final Statement statement : list)
                {
                    try
                    {
                        this.ensureSession().execute(statement);
                    }
                    catch (final Exception e)
                    {
                        throw new NormandraException("Unable to execute cql3 statement during database refresh.", e);
                    }
                }
            }
        }
    }


    @Override
    synchronized public void close()
    {
        if (this.session != null)
        {
            this.session.shutdown();
            this.session = null;
        }
        this.cluster.shutdown();
    }


    protected boolean hasTable(final String table)
    {
        final KeyspaceMetadata ksMeta = this.cluster.getMetadata().getKeyspace(this.keyspaceName);
        if (null == ksMeta)
        {
            return false;
        }
        final TableMetadata tblMeta = ksMeta.getTable(table);
        return tblMeta != null;
    }


    protected boolean hasColumn(final String table, final String column)
    {
        final KeyspaceMetadata ksMeta = this.cluster.getMetadata().getKeyspace(this.keyspaceName);
        if (null == ksMeta)
        {
            return false;
        }
        final TableMetadata tblMeta = ksMeta.getTable(table);
        if (null == tblMeta)
        {
            return false;
        }
        final ColumnMetadata colMeta = tblMeta.getColumn(column);
        return colMeta != null;
    }


    private static Statement defineTable(final String table, final Iterable<ColumnMeta> columns)
    {
        // define table
        final StringBuilder cql = new StringBuilder();
        cql.append("CREATE TABLE ").append(table).append(" (").append(IOUtils.LINE_SEPARATOR);

        // define columns
        boolean firstColumn = true;
        for (final ColumnMeta column : columns)
        {
            if (!firstColumn)
            {
                cql.append(",").append(IOUtils.LINE_SEPARATOR);
            }
            final String name = column.getName();
            final String type = CassandraUtils.columnType(column);
            cql.append(name).append(" ").append(type);
            firstColumn = false;
        }

        // define primary keys
        boolean firstKey = true;
        cql.append(",").append(IOUtils.LINE_SEPARATOR);
        cql.append("PRIMARY KEY (");
        for (final ColumnMeta column : columns)
        {
            if (column.isPrimaryKey())
            {
                if (!firstKey)
                {
                    cql.append(", ");
                }
                cql.append(column.getName());
                firstKey = false;
            }
        }
        cql.append(")").append(IOUtils.LINE_SEPARATOR);

        cql.append(");");
        return new SimpleStatement(cql.toString());
    }


    /**
     * per driver documentation, session is thread-safe and recommended for use on a per-keyspace basis
     */
    synchronized private Session ensureSession()
    {
        if (this.session != null)
        {
            return this.session;
        }
        this.ensureKeyspace();
        this.session = this.cluster.connect(this.keyspaceName);
        return this.session;
    }


    /**
     * ensure keyspace exists prior to constructing database session
     */
    synchronized private void ensureKeyspace()
    {
        final KeyspaceMetadata meta = this.cluster.getMetadata().getKeyspace(this.keyspaceName);
        if (meta != null)
        {
            return;
        }

        final Session session = this.cluster.connect();
        try
        {
            final StringBuilder cql = new StringBuilder();
            cql.append("CREATE KEYSPACE ").append(this.keyspaceName).append(" WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};");
            session.execute(cql.toString());
        }
        finally
        {
            session.shutdown();
        }
    }
}