package org.normandra;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.cassandra.CassandraDatabase;
import org.normandra.cassandra.CassandraDatabaseFactory;
import org.normandra.config.AnnotationParser;
import org.normandra.config.DatabaseMeta;
import org.normandra.config.EntityMeta;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * an entity manager backed by NoSQL database
 * <p/>
 * User: bowen
 * Date: 8/31/13
 */
public class NormandraEntityManager
{
    public class Builder
    {
        private DatabaseConstruction mode = DatabaseConstruction.CREATE;

        private ClassLoader classLoader = NormandraEntityManager.class.getClassLoader();

        private final Set<Class<?>> classes = new HashSet<>();

        private final SortedMap<String, Object> parameters = new TreeMap<>();


        public NormandraEntityManager create()
        {
            // read all entities
            final List<EntityMeta> entities = new LinkedList<>();
            for (final Class<?> clazz : this.classes)
            {
                final AnnotationParser parser = new AnnotationParser(clazz);
                final EntityMeta entity = parser.readEntity();
                if (entity != null)
                {
                    entities.add(entity);
                }
            }

            // setup database
            final DatabaseMeta meta = new DatabaseMeta(entities);

            // create entity manager
            final String keyspace = this.getParameter(CassandraDatabase.KEYSPACE, String.class, "normandra");
            final String hosts = this.getParameter(CassandraDatabase.HOSTS, String.class, CassandraDatabaseFactory.DEFAULT_HOST);
            final Integer port = this.getParameter(CassandraDatabase.PORT, Integer.class, CassandraDatabaseFactory.DEFAULT_PORT);
            final NormandraDatabaseFactory factory = new CassandraDatabaseFactory(keyspace, hosts, port.intValue(), this.mode);
            final NormandraDatabase database = factory.create();
            if (null == database)
            {
                return null;
            }
            database.refresh(meta);

            // success - create manager
            return new NormandraEntityManager(database, meta);
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


        public Builder withClassLoader(final ClassLoader cl)
        {
            if (null == cl)
            {
                throw new NullArgumentException("class loader");
            }
            this.classLoader = cl;
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
            this.classes.addAll(Arrays.asList(clazzes));
            return this;
        }


        public Builder withClasses(final Collection<Class<?>> c)
        {
            if (null == c)
            {
                throw new NullArgumentException("classes");
            }
            this.classes.addAll(c);
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

    private final NormandraDatabase database;

    private final DatabaseMeta meta;


    private NormandraEntityManager(final NormandraDatabase db, final DatabaseMeta meta)
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
    }

}
