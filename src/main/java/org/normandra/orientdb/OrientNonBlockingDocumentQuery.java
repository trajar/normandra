package org.normandra.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLNonBlockingQuery;
import org.apache.commons.lang.NullArgumentException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * a orientdb database query activity
 * <p>
 * Date: 4/4/14
 */
public class OrientNonBlockingDocumentQuery implements Iterable<ODocument>
{
    private final ODatabaseDocumentTx database;

    private final String query;

    private final List<Object> parameterList;

    private final Map<String, Object> parameterMap;

    public OrientNonBlockingDocumentQuery(final ODatabaseDocumentTx db, final String query, final Collection<?> params)
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

    public OrientNonBlockingDocumentQuery(final ODatabaseDocumentTx db, final String query, final Map<String, Object> params)
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

    public Iterator<ODocument> execute()
    {
        final OrientNonBlockingListener listener = new OrientNonBlockingListener(this.database);
        final OSQLNonBlockingQuery q = new OSQLNonBlockingQuery(this.query, listener);
        if (!parameterList.isEmpty())
        {
            database.command(q).execute(parameterList.toArray());
        }
        else if (!parameterMap.isEmpty())
        {
            database.command(q).execute(parameterMap);
        }
        else
        {
            database.command(q).execute();
        }
        return listener;
    }

    final int size()
    {
        int cnt = 0;
        for (final Object item : this)
        {
            if (item != null)
            {
                cnt++;
            }
        }
        return cnt;
    }

    @Override
    public Iterator<ODocument> iterator()
    {
        return this.execute();
    }
}
