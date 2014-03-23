package org.normandra.data;

import org.normandra.NormandraException;
import org.normandra.meta.ColumnMeta;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * basic id accessor
 * <p>
 * User: bowen
 * Date: 2/15/14
 */
public class CompositeIdAccessor extends FieldColumnAccessor implements IdAccessor
{
    private final Map<ColumnMeta, ColumnAccessor> accessors;


    public CompositeIdAccessor(final Field field, final Map<ColumnMeta, ColumnAccessor> m)
    {
        super(field);
        if (null == m || m.isEmpty())
        {
            throw new IllegalArgumentException("Accessors cannot be null/empty.");
        }
        this.accessors = new TreeMap<>(m);
    }


    @Override
    public Object fromEntity(final Object entity) throws NormandraException
    {
        if (null == entity)
        {
            return null;
        }
        try
        {
            return this.get(entity);
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to get field [" + this.getField().getName() + "] from entity [" + entity + "].", e);
        }
    }


    @Override
    public Map<String, Object> fromKey(final Object key) throws NormandraException
    {
        if (null == key)
        {
            return Collections.emptyMap();
        }
        if (!this.getField().getType().isInstance(key))
        {
            return Collections.emptyMap();
        }

        final Map<String, Object> map = new HashMap<>(this.accessors.size());
        for (final Map.Entry<ColumnMeta, ColumnAccessor> entry : this.accessors.entrySet())
        {
            final ColumnMeta column = entry.getKey();
            final ColumnAccessor accessor = entry.getValue();
            final Object value = accessor.getValue(key);
            if (value != null)
            {
                map.put(column.getName(), value);
            }
        }
        return Collections.unmodifiableMap(map);
    }


    @Override
    public Object toKey(final Map<String, Object> map) throws NormandraException
    {
        if (null == map || map.isEmpty())
        {
            return null;
        }
        try
        {
            final Object instance = this.getField().getType().newInstance();
            for (final Map.Entry<ColumnMeta, ColumnAccessor> entry : this.accessors.entrySet())
            {
                final ColumnMeta column = entry.getKey();
                final ColumnAccessor accessor = entry.getValue();
                final Object value = map.get(column.getName());
                if (value != null)
                {
                    accessor.setValue(instance, value, null);
                }
            }
            return instance;
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to instantiate composite key from field [" + this.getField() + "].", e);
        }
    }
}
