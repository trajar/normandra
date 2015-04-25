package org.normandra.orientdb;

import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import org.apache.commons.lang.NullArgumentException;
import org.normandra.log.DatabaseActivity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * a orientdb database query activity
 * <p>
 * User: bowen
 * Date: 4/4/14
 */
public class OrientQueryActivity implements DatabaseActivity, OCommandResultListener
{
    private static final Logger logger = LoggerFactory.getLogger(OrientQueryActivity.class);

    private static final Executor executor = Executors.newCachedThreadPool(new ThreadFactoryImpl());

    private final ODatabaseDocumentTx database;

    private final String query;

    private final List<Object> parameterList;

    private final Map<String, Object> parameterMap;

    private final AtomicBoolean finished = new AtomicBoolean(false);

    private final AtomicInteger count = new AtomicInteger(0);

    private final BlockingQueue<ODocument> queue = new ArrayBlockingQueue<>(10);

    private long start = -1;

    private long end = -1;

    private Date date;


    public OrientQueryActivity(final ODatabaseDocumentTx db, final String query, final Collection<?> params)
    {
        if (null == db)
        {
            throw new NullArgumentException("database");
        }
        if (null == query)
        {
            throw new NullArgumentException("query");
        }
        this.database = db;
        this.query = query;
        this.parameterMap = Collections.emptyMap();
        if (params != null && !params.isEmpty())
        {
            this.parameterList = new ArrayList<>(params);
        }
        else
        {
            this.parameterList = Collections.emptyList();
        }
    }


    public OrientQueryActivity(final ODatabaseDocumentTx db, final String query, final Map<String, Object> params)
    {
        if (null == db)
        {
            throw new NullArgumentException("database");
        }
        if (null == query)
        {
            throw new NullArgumentException("query");
        }
        this.database = db;
        this.query = query;
        this.parameterList = Collections.emptyList();
        if (params != null && !params.isEmpty())
        {
            this.parameterMap = new TreeMap<>(params);
        }
        else
        {
            this.parameterMap = Collections.emptyMap();
        }
    }


    @Override
    public Type getType()
    {
        return Type.SELECT;
    }


    @Override
    public long getDuration()
    {
        return this.end - this.start;
    }


    @Override
    public Date getDate()
    {
        return this.date;
    }


    @Override
    public CharSequence getInformation()
    {
        if (!this.parameterList.isEmpty())
        {
            return this.getType() + " query [" + this.query + "] with values " + this.parameterList + ".";
        }
        else if (!this.parameterMap.isEmpty())
        {
            return this.getType() + " query [" + this.query + "] with values " + this.parameterMap + ".";
        }
        else
        {
            return this.getType() + " query [" + this.query + "].";
        }
    }


    public Iterator<ODocument> execute()
    {
        this.queue.clear();
        this.finished.getAndSet(false);
        this.count.getAndSet(0);
        this.date = new Date();
        this.start = System.currentTimeMillis();
        this.end = -11;

        final OSQLAsynchQuery asynch = new OSQLAsynchQuery(this.query, this);
        final Runnable worker = new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    if (!parameterList.isEmpty())
                    {
                        database.command(asynch).execute(parameterList.toArray());
                    }
                    else if (!parameterMap.isEmpty())
                    {
                        database.command(asynch).execute(parameterMap);
                    }
                    else
                    {
                        database.command(asynch).execute();
                    }
                }
                catch(final Exception e)
                {
                    logger.warn("Unable to execute query.", e);
                }
            }
        };
        executor.execute(worker);

        return new Iterator<ODocument>()
        {
            @Override
            public boolean hasNext()
            {
                // loop until we know if results are empty
                while (!isEmpty())
                {
                    try
                    {
                        if (!queue.isEmpty())
                        {
                            return true;
                        }
                        Thread.sleep(100);
                    }
                    catch (final InterruptedException e)
                    {
                        logger.warn("Unable to poll next item from queue.", e);
                    }
                }
                // we are empty
                return false;
            }


            @Override
            public ODocument next()
            {
                try
                {
                    return queue.take();
                }
                catch (final InterruptedException e)
                {
                    logger.warn("Unable to take next item from queue.", e);
                    return null;
                }
            }


            private boolean isEmpty()
            {
                if (!finished.get())
                {
                    // we don't know if results are in or not yet
                    return false;
                }
                if (count.get() <= 0)
                {
                    // no results returned
                    return true;
                }
                if (!queue.isEmpty())
                {
                    // we have date left in queue
                    return false;
                }

                // nothing left to consume
                return true;
            }
        };
    }


    @Override
    public boolean result(final Object document)
    {
        if (null == document)
        {
            return false;
        }
        if (!(document instanceof ODocument))
        {
            return false;
        }
        try
        {
            this.count.incrementAndGet();
            this.queue.put((ODocument) document);
            return true;
        }
        catch (final Exception e)
        {
            logger.warn("Unable to add document to queue.", e);
            return false;
        }
    }


    @Override
    public void end()
    {
        this.end = System.currentTimeMillis();
        this.finished.getAndSet(true);
    }


    private static class ThreadFactoryImpl implements ThreadFactory
    {
        private final AtomicInteger count = new AtomicInteger();


        @Override
        public Thread newThread(final Runnable worker)
        {
            final Thread thread = new Thread(worker);
            thread.setDaemon(true);
            thread.setName(OrientQueryActivity.class.getSimpleName() + "-Query-" + count.incrementAndGet());
            return thread;
        }
    }
}
