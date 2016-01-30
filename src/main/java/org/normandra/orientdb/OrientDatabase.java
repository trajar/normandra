package org.normandra.orientdb;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.NullArgumentException;
import org.apache.commons.lang3.StringUtils;
import org.normandra.Database;
import org.normandra.DatabaseConstruction;
import org.normandra.NormandraException;
import org.normandra.cache.EntityCacheFactory;
import org.normandra.generator.IdGenerator;
import org.normandra.meta.AnnotationParser;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.DatabaseMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;
import org.normandra.util.ArraySet;
import org.normandra.util.CaseUtils;
import org.normandra.util.QueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.SequenceGenerator;
import javax.persistence.TableGenerator;
import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Date: 5/14/14
 */
public class OrientDatabase implements Database
{
    public static final String URL = "orientdb.url";

    public static final String USER_ID = "orientdb.userid";

    public static final String PASSWORD = "orientdb.password";

    private static final Logger logger = LoggerFactory.getLogger(OrientDatabase.class);

    private final String url;

    private final OrientPool pool;

    private final EntityCacheFactory cache;

    private final DatabaseConstruction constructionMode;

    private final Map<String, OrientQuery> statementsByName = new ConcurrentHashMap<>();

    public OrientDatabase(final String url, final OrientPool pool, final EntityCacheFactory cache, final DatabaseConstruction mode)
    {
        if (null == cache)
        {
            throw new NullArgumentException("cache factory");
        }
        if (null == mode)
        {
            throw new NullArgumentException("construction mode");
        }
        if (null == pool)
        {
            throw new NullArgumentException("pool");
        }
        this.url = url;
        this.pool = pool;
        this.cache = cache;
        this.constructionMode = mode;
    }

    final ODatabaseDocumentTx createDatabase()
    {
        return this.pool.acquire();
    }

    public boolean isLocal()
    {
        return this.url.toLowerCase().startsWith("plocal:") || this.url.toLowerCase().startsWith("local:");
    }

    @Override
    public OrientDatabaseSession createSession()
    {
        return new OrientDatabaseSession(this.createDatabase(), this.statementsByName, this.cache.create());
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

        if (this.isLocal())
        {
            // initialize directory structure as necessary
            final int index = this.url.indexOf(":");
            final File path = new File(this.url.substring(index + 1));
            try
            {
                if (!path.exists())
                {
                    FileUtils.forceMkdir(path);
                }
                if (path.list() == null || path.list().length <= 0)
                {
                    try (final ODatabase db = new ODatabaseDocumentTx(this.url, false))
                    {
                        db.create();
                    }
                }
            }
            catch (final Exception e)
            {
                throw new NormandraException("Unable to create local directory structure.", e);
            }
        }

        // setup entity schema
        try (final ODatabaseDocumentTx database = this.createDatabase())
        {
            for (final EntityMeta entity : meta.getEntities())
            {
                for (final TableMeta table : entity)
                {
                    this.refreshEntityWithTransaction(entity, table, database);
                }
                this.refreshGenerators(entity, database);
            }
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to refresh database.", e);
        }
    }

    private void refreshGenerators(final EntityMeta entity, final ODatabaseDocumentTx database)
    {
        // setup any table sequence/id generators
        final Class<?> entityType = entity.getType();
        final AnnotationParser parser = new AnnotationParser(new OrientAccessorFactory(), entityType);
        for (final Map.Entry<Field, GeneratedValue> entry : parser.getGenerators(entityType).entrySet())
        {
            final Field field = entry.getKey();
            final GeneratedValue generator = entry.getValue();
            final String type = generator.generator();
            String tableName = "id_generator";
            String keyColumn = "id";
            String keyValue = entity.getTables().size() == 1 ? entity.getTables().iterator().next().getName() : CaseUtils.camelToSnakeCase(entity.getName());
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
                throw new IllegalStateException("No support available for orient-db identity primary key generation.");
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
                throw new IllegalStateException("Unable to locate primary key [" + fieldName + "] for entity [" + entity + "].");
            }

            // drop table as required
            if (DatabaseConstruction.RECREATE.equals(this.constructionMode))
            {
                if (hasCluster(database, tableName))
                {
                    database.command(new OCommandSQL("DELETE FROM " + tableName)).execute();
                    database.getMetadata().getSchema().dropClass(tableName);
                }
                if (hasIndex(database, indexName))
                {
                    database.command(new OCommandSQL("DROP INDEX " + indexName)).execute();
                    database.getMetadata().getIndexManager().dropIndex(indexName);
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

            // assign generator
            final IdGenerator counter = new OrientIdGenerator(tableName, indexName, keyColumn, valueColumn, keyValue, this);
            entity.setGenerator(column, counter);
            logger.info("Set counter id generator for [" + column + "] on entity [" + entity + "].");
        }
    }

    private void refreshEntityWithTransaction(final EntityMeta entity, final TableMeta table, final ODatabaseDocumentTx database)
    {
        final String keyIndex = OrientUtils.keyIndex(table);
        final String schemaName = table.getName();

        if (DatabaseConstruction.RECREATE.equals(this.constructionMode))
        {
            // drop schema
            if (hasClass(database, schemaName))
            {
                database.command(new OCommandSQL("DELETE FROM " + schemaName)).execute();
            }
            if (hasIndex(database, keyIndex))
            {
                database.command(new OCommandSQL("DROP INDEX " + keyIndex)).execute();
            }
            for (final ColumnMeta column : entity.getIndexed())
            {
                final String propertyIndex = schemaName + "." + column.getName();
                if (hasIndex(database, propertyIndex))
                {
                    database.command(new OCommandSQL("DROP INDEX " + propertyIndex)).execute();
                }
            }
        }

        // create new class
        final OClass schemaClass = database.getMetadata().getSchema().getOrCreateClass(schemaName);
        final Set<ColumnMeta> primary = new ArraySet<>();
        for (final ColumnMeta column : table)
        {
            final String property = column.getName();
            final OType type = OrientUtils.columnType(column);
            if (type != null && !schemaClass.existsProperty(property))
            {
                schemaClass.createProperty(property, type);
            }
            if (column.isPrimaryKey())
            {
                primary.add(column);
            }
        }

        // create index as needed
        if (!hasIndex(database, keyIndex))
        {
            final Set<ColumnMeta> sortedKeys = new TreeSet<>(primary);
            final Collection<String> sortedNames = sortedKeys.stream().map(ColumnMeta::getName).collect(Collectors.toList());
            database.command(new OCommandSQL("CREATE INDEX " + keyIndex + " ON " + schemaName + " (" + StringUtils.join(sortedNames, ",") + ") UNIQUE")).execute();
        }
        for (final ColumnMeta column : entity.getIndexed())
        {
            final String propertyIndex = schemaName + "." + column.getName();
            if (!column.isPrimaryKey())
            {
                if (!hasIndex(database, propertyIndex))
                {
                    database.command(new OCommandSQL("CREATE INDEX " + propertyIndex + " NOTUNIQUE")).execute();
                }
            }
        }
    }

    @Override
    public boolean registerQuery(final EntityContext entity, final String name, final String query) throws NormandraException
    {
        if (null == name || name.isEmpty())
        {
            return false;
        }
        if (null == query || query.isEmpty())
        {
            return false;
        }
        if (this.statementsByName.containsKey(name))
        {
            return false;
        }

        final String tableQuery = QueryUtils.prepare(entity, query);
        if (null == tableQuery || tableQuery.isEmpty())
        {
            return false;
        }

        final OrientQuery prepared = new OrientQuery(query, tableQuery);
        this.statementsByName.put(name, prepared);
        return true;
    }

    @Override
    public boolean unregisterQuery(final String name) throws NormandraException
    {
        if (null == name || name.isEmpty())
        {
            return false;
        }
        return this.statementsByName.remove(name) != null;
    }

    private static Collection<String> getClasses(final ODatabaseDocument database)
    {
        final Collection<OClass> types = database.getMetadata().getSchema().getClasses();
        if (null == types || types.isEmpty())
        {
            return Collections.emptyList();
        }
        final List<String> list = new ArrayList<>(types.size());
        for (final OClass type : types)
        {
            list.add(type.getName());
        }
        return Collections.unmodifiableCollection(list);
    }

    private static Collection<String> getClusters(final ODatabaseDocument database)
    {
        final Collection<String> names = database.getClusterNames();
        if (null == names || names.isEmpty())
        {
            return Collections.emptyList();
        }
        return Collections.unmodifiableCollection(names);
    }

    private static Collection<String> getIndices(final ODatabaseDocument database)
    {
        final List<String> names = new ArrayList<>();
        for (final OIndex index : database.getMetadata().getIndexManager().getIndexes())
        {
            names.add(index.getName());
        }
        return Collections.unmodifiableCollection(names);
    }

    private static boolean hasIndex(final ODatabaseDocument database, final String clusterName)
    {
        if (null == clusterName || clusterName.isEmpty())
        {
            return false;
        }
        for (final String existing : getIndices(database))
        {
            if (clusterName.equalsIgnoreCase(existing))
            {
                return true;
            }
        }
        return false;
    }

    private static boolean hasCluster(final ODatabaseDocument database, final String clusterName)
    {
        if (null == clusterName || clusterName.isEmpty())
        {
            return false;
        }
        for (final String existing : getClusters(database))
        {
            if (clusterName.equalsIgnoreCase(existing))
            {
                return true;
            }
        }
        return false;
    }

    private static boolean hasClass(final ODatabaseDocument database, final String className)
    {
        if (null == className || className.isEmpty())
        {
            return false;
        }
        for (final String existing : getClasses(database))
        {
            if (className.equalsIgnoreCase(existing))
            {
                return true;
            }
        }
        return false;
    }

    private static boolean hasProperty(final ODatabaseDocument database, final String className, final String fieldName)
    {
        final OClass schemaClass = database.getMetadata().getSchema().getClass(className);
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

    @Override
    public void close()
    {
        this.pool.close();
    }
}
