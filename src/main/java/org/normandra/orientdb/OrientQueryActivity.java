package org.normandra.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.NullArgumentException;
import org.normandra.log.DatabaseActivity;

/**
 * a orientdb database query activity
 * <p>
 * User: bowen Date: 4/4/14
 */
public class OrientQueryActivity implements DatabaseActivity
{
    private final ODatabaseDocumentTx database;

    private final String query;

    private final List<Object> parameterList;

    private final Map<String, Object> parameterMap;

    private final AtomicInteger count = new AtomicInteger(0);

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
        this.count.getAndSet(0);
        this.date = new Date();
        this.start = System.currentTimeMillis();
        this.end = -1;

        final List<ODocument> docs;
        final OSQLSynchQuery q = new OSQLSynchQuery(this.query);
        if (!parameterList.isEmpty())
        {
            docs = database.command(q).execute(parameterList.toArray());
        }
        else if (!parameterMap.isEmpty())
        {
            docs = database.command(q).execute(parameterMap);
        }
        else
        {
            docs = database.command(q).execute();
        }
        this.end = System.currentTimeMillis();
        this.count.getAndSet(docs.size());
        return docs.iterator();
    }
}
