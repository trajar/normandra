package org.normandra.orientdb;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * User: bowen
 * Date: 3/9/16
 */
public class OrientNonBlockingListener implements OCommandResultListener, Iterator<ODocument>
{
    private static final Logger logger = LoggerFactory.getLogger(OrientNonBlockingDocumentQuery.class);

    private static final long defaultMaxWait = 1000 * 60 * 5;

    private final ODatabaseDocument database;

    private final long maxWait;

    private final BlockingQueue<Object> queue = new ArrayBlockingQueue<>(10);

    private ODocument next = null;

    private boolean done = false;

    private boolean needsFetch = true;

    public OrientNonBlockingListener(final ODatabaseDocument db)
    {
        this(db, defaultMaxWait);
    }

    public OrientNonBlockingListener(final ODatabaseDocument db, final long maxWait)
    {
        this.database = db;
        this.maxWait = maxWait;
    }

    @Override
    public boolean result(final Object record)
    {
        if (null == record)
        {
            return false;
        }

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

        return false;
    }

    @Override
    public void end()
    {
        this.endService();
    }

    private boolean endService()
    {
        if (this.isDone())
        {
            return false;
        }

        synchronized (this)
        {
            this.done = true;
            this.needsFetch = false;
        }

        logger.info("Adding end-of-service item to queue.");
        for (int i = 0; i < 10; i++)
        {
            try
            {
                if (queue.offer(new EndOfServiceElement(), 500, TimeUnit.MILLISECONDS))
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

    public void close()
    {
        synchronized (this)
        {
            this.done = true;
            this.needsFetch = false;
        }

        this.queue.clear();
    }

    public boolean isDone()
    {
        synchronized (this)
        {
            return this.done;
        }
    }

    @Override
    public boolean hasNext()
    {
        if (this.needsFetch)
        {
            this.fetch();
        }
        return this.next != null;
    }

    @Override
    public ODocument next()
    {
        if (this.needsFetch)
        {
            this.fetch();
        }
        final ODocument document = this.next;
        this.next = null;
        this.needsFetch = true;
        return document;
    }

    private boolean fetch()
    {
        final long start = System.currentTimeMillis();
        while (!queue.isEmpty() || (!this.isDone() && maxWait >= (System.currentTimeMillis() - start)))
        {
            try
            {
                final Object item = queue.poll(100, TimeUnit.MILLISECONDS);
                if (item instanceof EndOfServiceElement)
                {
                    logger.debug("Found end-of-service item [" + ((EndOfServiceElement) item).guid + "].");
                    this.next = null;
                    this.needsFetch = false;
                    return false;
                }
                else if (item != null)
                {
                    this.next = this.buildDocument(item);
                    if (this.next != null)
                    {
                        this.needsFetch = false;
                        return true;
                    }
                }
            }
            catch (final OException e)
            {
                logger.debug("Unable to build document from queue item.", e);
            }
            catch (final InterruptedException e)
            {
                logger.debug("Unable to poll non-blocking queue.", e);
            }
        }
        return false;
    }

    private ODocument buildDocument(final Object item) throws OException
    {
        if (null == item)
        {
            return null;
        }

        if (item instanceof ODocument)
        {
            return (ODocument) item;
        }

        if (item instanceof OIdentifiable)
        {
            final ORecord record = this.database.getRecord((OIdentifiable) item);
            if (record instanceof ODocument)
            {
                return (ODocument) record;
            }
        }

        return null;
    }

    private static class EndOfServiceElement
    {
        private final UUID guid = UUID.randomUUID();
    }
}
