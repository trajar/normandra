package org.normandra;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.cassandra.CassandraDatabase;
import org.normandra.cassandra.CassandraDatabaseFactory;
import org.normandra.meta.AnnotationParser;
import org.normandra.meta.DatabaseMeta;
import org.normandra.meta.EntityMeta;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * entity manager factory
 * <p/>
 * User: bowen
 * Date: 2/1/14
 */
public class EntityManagerFactory
{
    public static class Builder
    {
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
            final String keyspace = this.getParameter(CassandraDatabase.KEYSPACE, String.class, "normandra");
            final String hosts = this.getParameter(CassandraDatabase.HOSTS, String.class, CassandraDatabaseFactory.DEFAULT_HOST);
            final Integer port = this.getParameter(CassandraDatabase.PORT, Integer.class, CassandraDatabaseFactory.DEFAULT_PORT);
            final DatabaseFactory factory = new CassandraDatabaseFactory(keyspace, hosts, port.intValue(), this.mode);
            final Database database = factory.create();
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

            // success - create manager
            return new EntityManagerFactory(database, meta);
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


    public EntityManager create() throws NormandraException
    {
        final DatabaseSession session = this.database.createSession();
        return new EntityManager(session, this.meta);
    }
}
