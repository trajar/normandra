package org.normandra.association;

import javassist.util.proxy.MethodHandler;
import org.apache.commons.lang.NullArgumentException;
import org.normandra.DatabaseSession;
import org.normandra.NormandraException;
import org.normandra.data.ColumnAccessor;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * lazy loaded object handler
 * <p/>
 * User: bowen
 * Date: 2/2/14
 */
public class LazyAssociationHandler implements MethodHandler
{
    private static final Logger logger = LoggerFactory.getLogger(LazyAssociationHandler.class);

    private final AtomicBoolean loaded = new AtomicBoolean(false);

    private final EntityMeta meta;

    private final AssociationAccessor accessor;

    private final DatabaseSession session;


    public LazyAssociationHandler(final EntityMeta meta, final AssociationAccessor accessor, final DatabaseSession session)
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
            final Object selfcasted = this.meta.getType().cast(self);
            this.load(selfcasted);
        }
        return proceed.invoke(self, args);
    }


    synchronized private boolean load(final Object self) throws NormandraException
    {
        if (this.loaded.get())
        {
            return false;
        }

        logger.info("Intercepting proxy method invocation, fetching lazy association for [" + this.meta + "].");
        final Object value = this.accessor.get();
        if (null == value)
        {
            logger.info("Lazy association fetched, null/empty value found.");
            return false;
        }

        final Object entity = this.meta.getType().cast(value);
        logger.info("Lazy association fetched, copying entity values from instance [" + entity + "].");
        for (final Map.Entry<ColumnMeta, ColumnAccessor> entry : this.meta.getAccessors())
        {
            final ColumnAccessor accessor = entry.getValue();
            final Object columnValue = accessor.getValue(entity);
            if (columnValue != null)
            {
                accessor.setValue(self, columnValue, this.session);
            }
        }

        this.loaded.getAndSet(true);
        return true;
    }
}
