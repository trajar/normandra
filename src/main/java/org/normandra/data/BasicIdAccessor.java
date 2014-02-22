package org.normandra.data;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.NormandraException;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * a simple id accessor based on a single primary key
 * <p/>
 * User: bowen
 * Date: 2/15/14
 */
public class BasicIdAccessor extends FieldColumnAccessor implements IdAccessor
{
    private final String primary;


    public BasicIdAccessor(final Field field, final String key)
    {
        super(field);
        if (null == key)
        {
            throw new NullArgumentException("primary key");
        }
        this.primary = key;
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
        final Map<String, Object> map = new HashMap<>(1);
        map.put(this.primary, key);
        return Collections.unmodifiableMap(map);
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        BasicIdAccessor that = (BasicIdAccessor) o;

        if (primary != null ? !primary.equals(that.primary) : that.primary != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (primary != null ? primary.hashCode() : 0);
        return result;
    }
}
