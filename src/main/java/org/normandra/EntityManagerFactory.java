package org.normandra;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.cassandra.CassandraDatabase;
import org.normandra.cassandra.CassandraDatabaseFactory;
import org.normandra.meta.AnnotationParser;
import org.normandra.meta.DatabaseMeta;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.HierarchyEntityContext;
import org.normandra.meta.QueryMeta;
import org.normandra.meta.SingleEntityContext;
import org.normandra.orientdb.OrientDatabase;
import org.normandra.orientdb.OrientDatabaseFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * entity manager factory
 * <p/>
 * User: bowen Date: 2/1/14
 */
public class EntityManagerFactory
{
    public static enum Type
    {
        CASSANDRA, ORIENTDB;

        public static Type parse(final String value)
        {
            if (value == null || value.isEmpty())
            {
                return null;
            }
            final String trimmed = value.trim();
            for (final Type type : Type.values())
            {
                if (trimmed.equalsIgnoreCase(type.toString()))
                {
                    return type;
                }
                if (trimmed.equalsIgnoreCase(type.name()))
                {
                    return type;
                }
            }
            return null;
        }
    }

    public static class Builder
    {
        private Type type = Type.CASSANDRA;

        private DatabaseConstruction mode = DatabaseConstruction.CREATE;

        private final Set<Class> classes = new HashSet<>();

        private final SortedMap<String, Object> parameters = new TreeMap<>();

        public EntityManagerFactory create() throws NormandraException
        {
            // read all entities
            final AnnotationParser parser = new AnnotationParser(this.classes);
            final Collection<EntityMeta> entities = parser.read();

            // setup database
            final DatabaseMeta meta = new DatabaseMeta(entities);

            // create entity manager
            DatabaseFactory databaseFactory = null;
            if (Type.CASSANDRA.equals(type))
            {
                final String keyspace = this.getParameter(CassandraDatabase.KEYSPACE, String.class, "normandra");
                final String hosts = this.getParameter(CassandraDatabase.HOSTS, String.class, CassandraDatabaseFactory.DEFAULT_HOST);
                final Integer port = this.getParameter(CassandraDatabase.PORT, Integer.class, CassandraDatabaseFactory.DEFAULT_PORT);
                databaseFactory = new CassandraDatabaseFactory(keyspace, hosts, port.intValue(), this.mode);
            }
            else if (Type.ORIENTDB.equals(type))
            {
                final String url = this.getParameter(OrientDatabase.URL, String.class, "plocal:orientdb");
                final String userid = this.getParameter(OrientDatabase.USER_ID, String.class, "admin");
                final String password = this.getParameter(OrientDatabase.PASSWORD, String.class, "admin");
                databaseFactory = new OrientDatabaseFactory(url, userid, password, this.mode);
            }
            else
            {
                throw new IllegalArgumentException("Unknown database type [" + type + "].");
            }
            final Database database = databaseFactory.create();
            if (null == database)
            {
                return null;
            }
            try
            {
                database.refresh(meta);
            }
            catch (final Exception e)
            {
                database.close();
                throw new NormandraException("Unable to referesh database.", e);
            }

            // create manager, register queries
            final EntityManagerFactory managerFactory = new EntityManagerFactory(database, meta);
            for (final Class<?> clazz : this.classes)
            {
                for (final QueryMeta query : parser.getQueries(clazz))
                {
                    managerFactory.registerQuery(query.getEntity(), query.getName(), query.getQuery());
                }
            }
            return managerFactory;
        }

        public Builder withType(final Type type)
        {
            if (null == type)
            {
                throw new NullArgumentException("factory type");
            }
            this.type = type;
            return this;
        }

        public Builder withDatabaseConstruction(final DatabaseConstruction mode)
        {
            if (null == mode)
            {
                throw new NullArgumentException("database construction mode");
            }
            this.mode = mode;
            return this;
        }

        public Builder withClass(final Class<?> clazz)
        {
            if (null == clazz)
            {
                throw new NullArgumentException("class");
            }
            this.classes.add(clazz);
            return this;
        }

        public Builder withClasses(final Class<?>... clazzes)
        {
            if (null == clazzes)
            {
                throw new NullArgumentException("classes");
            }
            for (final Class<?> clazz : clazzes)
            {
                this.withClass(clazz);
            }
            return this;
        }

        public Builder withClasses(final Iterable<Class<?>> c)
        {
            if (null == c)
            {
                throw new NullArgumentException("classes");
            }
            for (final Class<?> clazz : c)
            {
                this.withClass(clazz);
            }
            return this;
        }

        public Builder withParameter(final String key, final Object value)
        {
            if (null == key || null == value)
            {
                throw new NullArgumentException("key/value");
            }
            this.parameters.put(key, value);
            return this;
        }

        private <T> T getParameter(final String key, final Class<T> clazz, final T defaultValue)
        {
            if (null == key)
            {
                return null;
            }
            final Object value = this.parameters.get(key);
            if (null == value)
            {
                return defaultValue;
            }
            return clazz.cast(value);
        }
    }

    private final Database database;

    private final DatabaseMeta meta;

    private final Map<Class, EntityMeta> classMap;

    private EntityManagerFactory(final Database db, final DatabaseMeta meta)
    {
        if (null == db)
        {
            throw new NullArgumentException("database");
        }
        if (null == meta)
        {
            throw new NullArgumentException("metadata");
        }
        this.database = db;
        this.meta = meta;

        final int size = meta.getEntities().size();
        this.classMap = new HashMap<>(size);
        for (final EntityMeta entity : this.meta)
        {
            this.classMap.put(entity.getType(), entity);
        }
    }

    public EntityManager create()
    {
        final DatabaseSession session = this.database.createSession();
        return new EntityManager(session, this.meta);
    }

    public <T> boolean registerQuery(final Class<T> clazz, final String name, final String query) throws NormandraException
    {
        if (null == clazz)
        {
            throw new NullArgumentException("type");
        }

        final List<EntityMeta> list = this.findMeta(clazz);
        if (list.isEmpty())
        {
            return false;
        }

        if (list.size() == 1)
        {
            return this.database.registerQuery(new SingleEntityContext(list.get(0)), name, query);
        }
        else
        {
            return this.database.registerQuery(new HierarchyEntityContext(list), name, query);
        }
    }

    public boolean unregisterQuery(final String name) throws NormandraException
    {
        return this.database.unregisterQuery(name);
    }

    public void close()
    {
        this.database.close();
        this.classMap.clear();
    }

    private List<EntityMeta> findMeta(final Class<?> clazz)
    {
        final EntityMeta existing = this.classMap.get(clazz);
        if (existing != null)
        {
            return Arrays.asList(existing);
        }
        final List<EntityMeta> list = new ArrayList<>();
        for (final Map.Entry<Class, EntityMeta> entry : this.classMap.entrySet())
        {
            final Class<?> entityClass = entry.getKey();
            final EntityMeta entityMeta = entry.getValue();
            if (clazz.isAssignableFrom(entityClass))
            {
                list.add(entityMeta);
            }
        }
        return Collections.unmodifiableList(list);
    }
}
