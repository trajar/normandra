package org.normandra.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.apache.commons.lang.NullArgumentException;
import org.normandra.Database;
import org.normandra.DatabaseConstruction;
import org.normandra.DatabaseSession;
import org.normandra.NormandraException;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.DatabaseMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;
import org.normandra.util.ArraySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * User: bowen
 * Date: 5/14/14
 */
public class OrientDatabase implements Database
{
    private static final Logger logger = LoggerFactory.getLogger(OrientDatabase.class);

    private final String url;

    private final String userId;

    private final String password;

    private final DatabaseConstruction constructionMode;


    public OrientDatabase(final String url, final DatabaseConstruction mode)
    {
        this(url, null, null, mode);
    }


    public OrientDatabase(final String url, final String user, final String pwd, final DatabaseConstruction mode)
    {
        if (null == url || url.isEmpty())
        {
            throw new IllegalArgumentException("URL cannot be null/empty.");
        }
        if (null == mode)
        {
            throw new NullArgumentException("construction mode");
        }
        this.url = url;
        this.userId = user;
        this.password = pwd;
        this.constructionMode = mode;
    }


    private ODatabaseDocumentTx createDatabase()
    {
        final ODatabaseDocumentTx db = new ODatabaseDocumentTx(this.url);
        if (this.userId != null || this.password != null)
        {
            return db.open(this.userId, this.password);
        }
        else
        {
            return db;
        }
    }


    @Override
    public DatabaseSession createSession()
    {
        return new OrientDatabaseSession(this.createDatabase());
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

        for (final EntityMeta entity : meta.getEntities())
        {
            final ODatabaseDocumentTx database = this.createDatabase();
            try
            {
                this.refreshEntity(entity, database);
            }
            catch (final Exception e)
            {
                throw new NormandraException("Unable to refresh entity [" + entity + "].", e);
            }
            finally
            {
                database.close();
            }
        }
    }


    private void refreshEntity(final EntityMeta meta, final ODatabaseDocumentTx database)
    {
        final String schemaName = meta.getName();

        if (meta.getTables().size() > 1)
        {
            logger.warn("Collapsing schema for entity [" + meta + "] - only one table per document supported.");
        }

        if (this.hasCluster(schemaName) && DatabaseConstruction.RECREATE.equals(this.constructionMode))
        {
            // drop schema
            database.command(new OCommandSQL("DELETE FROM " + schemaName)).execute();
            database.getMetadata().getSchema().dropClass(schemaName);
        }

        // create new class schema
        final OClass schemaClass = database.getMetadata().getSchema().getOrCreateClass(schemaName);
        final Set<String> primary = new ArraySet<>();
        for (final TableMeta table : meta)
        {
            for (final ColumnMeta column : table)
            {
                schemaClass.createProperty(column.getProperty(), OrientUtils.columnType(column));
                if (column.isPrimaryKey())
                {
                    primary.add(column.getProperty());
                }
            }
        }

        // create index
        final String indexName = OrientUtils.keyIndex(meta);
        final String[] keys = primary.toArray(new String[primary.size()]);
        schemaClass.createIndex(indexName, OClass.INDEX_TYPE.UNIQUE, keys);
    }


    @Override
    public boolean registerQuery(EntityContext meta, String name, String query) throws NormandraException
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean unregisterQuery(String name) throws NormandraException
    {
        throw new UnsupportedOperationException();
    }


    public Collection<String> getClusters()
    {
        final ODatabaseDocumentTx database = this.createDatabase();
        try
        {
            final Collection<String> names = database.getClusterNames();
            if (null == names || names.isEmpty())
            {
                return Collections.emptyList();
            }
            return Collections.unmodifiableCollection(names);
        }
        finally
        {
            database.close();
        }
    }


    public boolean hasCluster(final String clusterName)
    {
        if (null == clusterName || clusterName.isEmpty())
        {
            return false;
        }
        for (final String existing : this.getClusters())
        {
            if (clusterName.equalsIgnoreCase(existing))
            {
                return true;
            }
        }
        return false;
    }


    public boolean hasProperty(final String clusterName, final String fieldName)
    {
        final ODatabaseDocumentTx database = this.createDatabase();
        try
        {
            final OClass schemaClass = database.getMetadata().getSchema().getClass(clusterName);
            if (null == schemaClass)
            {
                return false;
            }
            final OProperty property = schemaClass.getProperty(fieldName);
            if (null == property)
            {
                return false;
            }
            return property.getType() != null;
        }
        finally
        {
            database.close();
        }
    }


    @Override
    public void close()
    {
        // nothing to do
    }
}
