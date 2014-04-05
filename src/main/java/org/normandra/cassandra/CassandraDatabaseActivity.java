package org.normandra.cassandra;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.normandra.log.DatabaseActivity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * a cassandra database activity, backed by a cql3 statement
 * <p>
 * User: bowen
 * Date: 4/4/14
 */
public class CassandraDatabaseActivity implements DatabaseActivity
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraDatabaseActivity.class);

    private final RegularStatement statement;

    private final Session session;

    private final Type type;

    private final AtomicBoolean success = new AtomicBoolean(false);

    private ResultSet results;

    private long duration = -1;

    private Date date;


    public CassandraDatabaseActivity(final RegularStatement statement, final Session session, final Type type)
    {
        this.statement = statement;
        this.session = session;
        this.type = type;
    }


    @Override
    public Type getType()
    {
        return this.type;
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
        return this.statement.getQueryString();
    }


    synchronized public ResultSet execute()
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
