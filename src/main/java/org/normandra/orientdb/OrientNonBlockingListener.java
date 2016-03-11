package org.normandra.orientdb;

import com.orientechnologies.orient.core.command.OCommandResultListener;
import org.apache.commons.lang.NullArgumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * User: bowen
 * Date: 3/9/16
 */
public class OrientNonBlockingListener<T> implements OCommandResultListener, Iterator<T>
{
    private static final Logger logger = LoggerFactory.getLogger(OrientNonBlockingDocumentQuery.class);

    private static final long defaultMaxWait = 1000 * 60 * 5;

    private final long maxWait;

    private final Class<T> clazz;

    private final BlockingQueue<Object> queue = new ArrayBlockingQueue<>(10);

    private T next = null;

    private boolean done = false;

    public OrientNonBlockingListener(final Class<T> clazz)
    {
        this(clazz, defaultMaxWait);
    }

    public OrientNonBlockingListener(final Class<T> clazz, final long maxWait)
    {
        if (null == clazz)
        {
            throw new NullArgumentException("clazz");
        }
        this.clazz = clazz;
        this.maxWait = maxWait;
    }

    @Override
    public boolean result(final Object record)
    {
        if (null == record)
        {
            return false;
        }

        if (this.clazz.isInstance(record))
        {
            final long start = System.currentTimeMillis();
            while (maxWait >= (System.currentTimeMillis() - start))
            {
                try
                {
                    if (queue.offer(record, 100, TimeUnit.MILLISECONDS))
                    {
                        return true;
                    }
                }
                catch (final InterruptedException e)
                {
                    logger.debug("Unable to offer document in non-blocking queue.", e);
                }
            }
        }

        return false;
    }

    public void close()
    {
        synchronized (this)
        {
            this.done = true;
        }

        this.queue.clear();
    }

    @Override
    public void end()
    {
        synchronized (this)
        {
            this.done = true;
        }

        this.addEndOfServiceItem();
    }

    public boolean isDone()
    {
        synchronized (this)
        {
            return this.done;
        }
    }

    private boolean addEndOfServiceItem()
    {
        for (int i = 0; i < 10; i++)
        {
            try
            {
                if (queue.offer(Boolean.FALSE, 500, TimeUnit.MILLISECONDS))
                {
                    return true;
                }
            }
            catch (final InterruptedException e)
            {
                logger.debug("Unable to offer document in non-blocking queue.", e);
            }
        }

        logger.info("Unable to add end-of-service item to queue.");
        return false;
    }

    @Override
    public boolean hasNext()
    {
        while (!this.isDone())
        {
            try
            {
                System.out.print("waiting for queue at.");
                final Object item = queue.poll(100, TimeUnit.MILLISECONDS);
                if (item != null)
                {
                    if (this.clazz.isInstance(item))
                    {
                        this.next = this.clazz.cast(item);
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
            }
            catch (final InterruptedException e)
            {
                logger.debug("Unable to poll non-blocking queue.", e);
            }
        }

        return false;
    }

    @Override
    public T next()
    {
        return this.next;
    }
}
