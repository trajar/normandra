package org.normandra;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.cassandra.CassandraDatabase;
import org.normandra.cassandra.CassandraDatabaseFactory;
import org.normandra.meta.AnnotationParser;
import org.normandra.meta.DatabaseMeta;
import org.normandra.meta.EntityMeta;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
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
public class NormandraEntityManagerFactory
{
    public class Builder
    {
        private DatabaseConstruction mode = DatabaseConstruction.CREATE;

        private final Set<Class<?>> classes = new HashSet<>();

        private final SortedMap<String, Object> parameters = new TreeMap<>();


        public NormandraEntityManagerFactory create() throws NormandraException
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
            return new NormandraEntityManagerFactory(database, meta);
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

    private final Map<Class<?>, EntityMeta> classMap;


    private NormandraEntityManagerFactory(final NormandraDatabase db, final DatabaseMeta meta)
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


    public NormandraEntityManager create() throws NormandraException
    {
        final NormandraDatabaseSession session = this.database.createSession();
        return new NormandraEntityManager(session, this.meta);
    }
}
