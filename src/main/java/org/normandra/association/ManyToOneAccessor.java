package org.normandra.association;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.EntitySession;
import org.normandra.NormandraException;
import org.normandra.meta.EntityMeta;

/**
 * a single entity accessor - for one-to-one or many-to-one n relationships
 * <p/>
 * User: bowen
 * Date: 2/9/14
 */
public class ManyToOneAccessor<T> implements AssociationAccessor
{
    private final EntityMeta<T> meta;

    private final Object key;

    private final EntitySession session;


    public ManyToOneAccessor(final EntityMeta<T> meta, final Object key, final EntitySession session)
    {
        if (null == meta)
        {
            throw new NullArgumentException("entity meta");
        }
        if (null == key)
        {
            throw new NullArgumentException("key");
        }
        if (null == session)
        {
            throw new NullArgumentException("session");
        }
        this.meta = meta;
        this.key = key;
        this.session = session;
    }


    @Override
    public Object get() throws NormandraException
    {
        return this.session.get(this.meta, this.key);
    }
}
