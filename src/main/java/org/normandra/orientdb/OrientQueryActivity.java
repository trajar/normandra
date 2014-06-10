package org.normandra.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.apache.commons.lang.NullArgumentException;
import org.normandra.log.DatabaseActivity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * a orientdb database query activity
 * <p/>
 * User: bowen
 * Date: 4/4/14
 */
public class OrientQueryActivity implements DatabaseActivity, Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(OrientQueryActivity.class);

    private final ODatabaseDocumentTx database;

    private final String query;

    private final List<Object> parameterList;

    private final Map<String, Object> parameterMap;

    private List<ODocument> results = Collections.emptyList();

    private final AtomicBoolean finished = new AtomicBoolean(false);

    private long duration = -1;

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


    public List<ODocument> getResults()
    {
        return Collections.unmodifiableList(this.results);
    }


    public boolean isFinished()
    {
        return this.finished.get();
    }


    @Override
    public Type getType()
    {
        return Type.SELECT;
    }


    @Override
    public long getDuration()
    {
        return this.duration;
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


    @Override
    public void run()
    {
        try
        {
            this.execute();
        }
        catch (final Exception e)
        {
            logger.warn("Unable to query database.", e);
        }
    }


    public List<ODocument> execute()
    {
        this.finished.getAndSet(false);
        this.duration = -1;
        final long start = System.currentTimeMillis();
        this.date = new Date();
        if (!this.parameterList.isEmpty())
        {
            this.results = this.database.query(new OSQLSynchQuery<>(this.query), this.parameterList.toArray());
        }
        else if (!this.parameterMap.isEmpty())
        {
            this.results = this.database.query(new OSQLSynchQuery<>(this.query), this.parameterMap);
        }
        else
        {
            this.results = this.database.query(new OSQLSynchQuery<>(this.query));
        }
        this.duration = System.currentTimeMillis() - start;
        this.finished.getAndSet(true);
        return Collections.unmodifiableList(this.results);
    }
}
