package org.normandra.association;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.EntitySession;
import org.normandra.NormandraException;

/**
 * a single entity accessor - for one-to-one or many-to-one n relationships
 * <p>
 * User: bowen
 * Date: 2/9/14
 */
public class ManyToOneAccessor implements AssociationAccessor
{
    private final ElementIdentity factory;

    private final Object key;

    private final EntitySession session;


    public ManyToOneAccessor(final Object key, final EntitySession session, final ElementIdentity factory)
    {
        if (null == key)
        {
            throw new NullArgumentException("key");
        }
        if (null == session)
        {
            throw new NullArgumentException("session");
        }
        if (null == factory)
        {
            throw new NullArgumentException("factory");
        }
        this.key = key;
        this.session = session;
        this.factory = factory;
    }


    @Override
    public Object get() throws NormandraException
    {
        return this.factory.toEntity(this.session, this.key);
    }
}
