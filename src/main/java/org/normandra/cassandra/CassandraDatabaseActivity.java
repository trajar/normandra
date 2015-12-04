package org.normandra.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * a cassandra database listActivity, backed by a cql3 statement
 * <p>
 * Date: 4/4/14
 */
public class CassandraDatabaseActivity
{
    private final Statement statement;

    private final Session session;

    private final AtomicBoolean success = new AtomicBoolean(false);

    private ResultSet results;

    private long duration = -1;

    private Date date;

    public CassandraDatabaseActivity(final Statement statement, final Session session)
    {
        this.statement = statement;
        this.session = session;
    }

    public long getDuration()
    {
        return this.duration;
    }

    public Date getDate()
    {
        return this.date;
    }

    public ResultSet execute()
    {
        if (this.results != null)
        {
            return this.results;
        }
        this.date = new Date();
        final long start = System.currentTimeMillis();
        this.results = this.session.execute(this.statement);
        this.success.getAndSet(true);
        final long end = System.currentTimeMillis();
        this.duration = end - start;
        return this.results;
    }
}
