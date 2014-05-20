package org.normandra.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.apache.commons.lang.NullArgumentException;
import org.normandra.Database;
import org.normandra.DatabaseConstruction;
import org.normandra.NormandraException;
import org.normandra.generator.IdGenerator;
import org.normandra.meta.AnnotationParser;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.DatabaseMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.SingleEntityContext;
import org.normandra.meta.TableMeta;
import org.normandra.util.ArraySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.SequenceGenerator;
import javax.persistence.TableGenerator;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

    private final ODatabaseDocumentPool pool = createPool();


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


    protected final ODatabaseDocumentTx createDatabase()
    {
        return this.pool.acquire(this.url, this.userId, this.password);
    }


    @Override
    public OrientDatabaseSession createSession()
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

        // setup entity schema
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

        // setup any table sequence/id generators
        for (final EntityMeta entity : meta)
        {
            final Class<?> entityType = entity.getType();
            final AnnotationParser parser = new AnnotationParser(entityType);
            for (final Map.Entry<Field, GeneratedValue> entry : parser.getGenerators(entityType).entrySet())
            {
                final Field field = entry.getKey();
                final GeneratedValue generator = entry.getValue();
                final String type = generator.generator();
                String tableName = "id_generator";
                String keyColumn = "id";
                String keyValue = OrientUtils.schemaName(entity);
                String valueColumn = "value";

                if (GenerationType.TABLE.equals(generator.strategy()))
                {
                    for (final TableGenerator table : parser.findAnnotations(entityType, TableGenerator.class))
                    {
                        if (type.equalsIgnoreCase(table.name()))
                        {
                            if (!table.table().isEmpty())
                            {
                                tableName = table.table();
                            }
                            if (!table.pkColumnName().isEmpty())
                            {
                                keyColumn = table.pkColumnName();
                            }
                            if (!table.pkColumnValue().isEmpty())
                            {
                                keyValue = table.pkColumnValue();
                            }
                            if (!table.valueColumnName().isEmpty())
                            {
                                valueColumn = table.valueColumnName();
                            }
                        }
                    }
                }
                else if (GenerationType.SEQUENCE.equals(generator.strategy()))
                {
                    for (final SequenceGenerator sequence : parser.findAnnotations(entityType, SequenceGenerator.class))
                    {
                        if (type.equalsIgnoreCase(sequence.name()))
                        {
                            if (!sequence.sequenceName().isEmpty())
                            {
                                keyValue = sequence.sequenceName();
                            }
                        }
                    }
                }
                else if (GenerationType.IDENTITY.equals(generator.strategy()))
                {
                    throw new NormandraException("No support available for orient-db identity primary key generation.");
                }

                // get the column type
                final String fieldName = field.getName();
                final String indexName = tableName + "." + keyColumn;
                ColumnMeta column = null;
                for (final TableMeta table : entity)
                {
                    column = table.getColumn(fieldName);
                    if (column != null)
                    {
                        break;
                    }
                }
                if (null == column)
                {
                    throw new NormandraException("Unable to locate primary key [" + fieldName + "] for entity [" + entity + "].");
                }

                final ODatabaseDocumentTx database = this.createDatabase();
                try
                {
                    // drop table as required
                    if (DatabaseConstruction.RECREATE.equals(this.constructionMode))
                    {
                        if (this.hasCluster(tableName))
                        {
                            database.command(new OCommandSQL("DELETE FROM " + tableName)).execute();
                        }
                        if (database.getMetadata().getSchema().getClass(tableName) != null)
                        {
                            database.getMetadata().getSchema().dropClass(tableName);
                        }
                    }

                    // create sequence schema
                    final OClass schemaClass = database.getMetadata().getSchema().getOrCreateClass(tableName);
                    if (!schemaClass.existsProperty(keyColumn))
                    {
                        schemaClass.createProperty(keyColumn, OType.STRING);
                    }
                    if (!schemaClass.existsProperty(valueColumn))
                    {
                        schemaClass.createProperty(valueColumn, OrientUtils.columnType(column));
                    }
                    if (!schemaClass.areIndexed(keyColumn))
                    {
                        schemaClass.createIndex(indexName, OClass.INDEX_TYPE.UNIQUE, keyColumn);
                    }
                }
                catch (final Exception e)
                {
                    throw new NormandraException("Unable to refresh id generator [" + generator.generator() + "].", e);
                }
                finally
                {
                    database.close();
                }

                // assign generator
                final IdGenerator counter = new OrientCounterIdGenerator(tableName, indexName, keyColumn, valueColumn, keyValue, this);
                entity.setGenerator(column, counter);
                logger.info("Set counter id generator for [" + column + "] on entity [" + entity + "].");
            }
        }
    }


    private void refreshEntity(final EntityMeta meta, final ODatabaseDocumentTx database)
    {
        final String indexName = OrientUtils.keyIndex(meta);
        final String schemaName = OrientUtils.schemaName(meta);

        if (DatabaseConstruction.RECREATE.equals(this.constructionMode))
        {
            // drop schema
            if (this.hasCluster(schemaName))
            {
                database.command(new OCommandSQL("DELETE FROM " + schemaName)).execute();
                database.getMetadata().getSchema().dropClass(schemaName);
            }
            if (this.hasIndex(indexName))
            {
                database.command(new OCommandSQL("DROP INDEX " + indexName)).execute();
                database.getMetadata().getIndexManager().dropIndex(indexName);
            }
        }

        // check cluster (ie table)
        final Collection<TableMeta> primaryTables = new SingleEntityContext(meta).getPrimaryTables();
        if (primaryTables.size() > 1)
        {
            logger.warn("Collapsing schema for entity [" + meta + "] - only one table/cluster per document supported.");
        }

        // create new class schema
        final OClass schemaClass = database.getMetadata().getSchema().getOrCreateClass(schemaName);
        final Set<String> primary = new ArraySet<>();
        for (final TableMeta table : meta)
        {
            for (final ColumnMeta column : table)
            {
                final String property = OrientUtils.propertyName(column);
                if (!schemaClass.existsProperty(property))
                {
                    schemaClass.createProperty(property, OrientUtils.columnType(column));
                }
                if (column.isPrimaryKey())
                {
                    primary.add(property);
                }
            }
        }

        if (!this.hasIndex(indexName))
        {
            // create index
            final String[] keys = primary.toArray(new String[primary.size()]);
            schemaClass.createIndex(indexName, OClass.INDEX_TYPE.UNIQUE, keys);
        }
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


    public Collection<String> getIndices()
    {
        final ODatabaseDocumentTx database = this.createDatabase();
        try
        {
            final List<String> names = new ArrayList<>();
            for (final OIndex index : database.getMetadata().getIndexManager().getIndexes())
            {
                names.add(index.getName());
            }
            return Collections.unmodifiableCollection(names);
        }
        finally
        {
            database.close();
        }
    }


    public boolean hasIndex(final String clusterName)
    {
        if (null == clusterName || clusterName.isEmpty())
        {
            return false;
        }
        for (final String existing : this.getIndices())
        {
            if (clusterName.equalsIgnoreCase(existing))
            {
                return true;
            }
        }
        return false;
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


    private static ODatabaseDocumentPool createPool()
    {
        final ODatabaseDocumentPool pool = new ODatabaseDocumentPool();
        pool.setup(0, 10);
        pool.setName(OrientDatabase.class.getSimpleName() + "-DocumentPoolThread");
        return pool;
    }
}
