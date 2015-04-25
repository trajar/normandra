package org.normandra.data;

import org.normandra.EntitySession;
import org.normandra.NormandraException;
import org.normandra.meta.EntityContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * a lazy data holder that queries entities by id
 * <p>
 * User: bowen
 * Date: 4/25/15
 */
public class FindByIdDataHolder implements DataHolder
{
    private final AtomicBoolean loaded = new AtomicBoolean(false);

    private final EntitySession session;

    private final EntityContext entity;

    private final List<?> keys;

    private List<?> items;


    public FindByIdDataHolder(final EntitySession session, final EntityContext entity, final Collection<?> keys)
    {
        this.session = session;
        this.entity = entity;
        this.keys = new ArrayList<>(keys);
    }


    @Override
    public boolean isEmpty()
    {
        try
        {
            return this.ensureResults().isEmpty();
        }
        catch (final Exception e)
        {
            throw new IllegalStateException("Unable to determine size of entity results.", e);
        }
    }


    @Override
    public Collection<?> get() throws NormandraException
    {
        return this.ensureResults();
    }


    private Collection<?> ensureResults() throws NormandraException
    {
        if (!this.loaded.get())
        {
            try
            {
                this.items = this.session.get(this.entity, this.keys.toArray());
                this.loaded.getAndSet(true);
            }
            catch (final Exception e)
            {
                throw new NormandraException("Unable to query lazy loaded results from [" + this.entity + "] with ids " + this.keys + ".", e);
            }
        }

        if (null == this.items || this.items.isEmpty())
        {
            return Collections.unmodifiableCollection(new ArrayList<>(0));
        }
        else
        {
            return Collections.unmodifiableCollection(this.items);
        }
    }
}
