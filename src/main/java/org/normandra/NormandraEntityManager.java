package org.normandra;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.meta.DatabaseMeta;
import org.normandra.meta.EntityMeta;

import java.util.HashMap;
import java.util.Map;

/**
 * an entity manager backed by NoSQL database
 * <p/>
 * User: bowen
 * Date: 8/31/13
 */
public class NormandraEntityManager
{
    private final NormandraDatabaseSession database;

    private final DatabaseMeta meta;

    private final Map<Class<?>, EntityMeta> classMap;


    protected NormandraEntityManager(final NormandraDatabaseSession db, final DatabaseMeta meta)
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


    public <T> void save(final T element) throws NormandraException
    {
        if (null == element)
        {
            throw new NullArgumentException("element");
        }

        final Class<?> clazz = element.getClass();
        final EntityMeta meta = this.classMap.get(clazz);
        if (null == meta)
        {
            throw new IllegalArgumentException("Element [" + element + "] is not a registered entity.");
        }

        this.database.save(meta, element);
    }
}
