package org.normandra.association;

import javassist.util.proxy.MethodHandler;
import org.apache.commons.lang.NullArgumentException;
import org.normandra.NormandraDatabaseSession;
import org.normandra.NormandraException;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityMeta;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * lazy loaded object handler
 * <p/>
 * User: bowen
 * Date: 2/2/14
 */
public class LazyAssociationHandler<T> implements MethodHandler
{
    private final AtomicBoolean loaded = new AtomicBoolean(false);

    private final EntityMeta<T> meta;

    private final AssociationAccessor accessor;

    private final NormandraDatabaseSession session;


    public LazyAssociationHandler(final EntityMeta<T> meta, final AssociationAccessor accessor, final NormandraDatabaseSession session)
    {
        if (null == meta)
        {
            throw new NullArgumentException("entity meta");
        }
        if (null == accessor)
        {
            throw new NullArgumentException("accessor");
        }
        if (null == session)
        {
            throw new NullArgumentException("session");
        }
        this.meta = meta;
        this.accessor = accessor;
        this.session = session;
    }


    @Override
    public Object invoke(final Object self, final Method thisMethod, final Method proceed, final Object[] args) throws Throwable
    {
        if (!this.loaded.get())
        {
            final T selfcasted = this.meta.getType().cast(self);
            this.load(selfcasted);
        }
        return proceed.invoke(self, args);
    }


    synchronized private boolean load(final T self) throws NormandraException
    {
        if (this.loaded.get())
        {
            return false;
        }

        final Object value = this.accessor.get();
        if (null == value)
        {
            return false;
        }

        final T entity = this.meta.getType().cast(value);
        if (entity != null)
        {
            for (final ColumnMeta column : this.meta)
            {
                final Object columnValue = column.getAccessor().getValue(entity);
                if (columnValue != null)
                {
                    column.getAccessor().setValue(self, columnValue, this.session);
                }
            }
        }

        this.loaded.getAndSet(true);
        return true;
    }
}
