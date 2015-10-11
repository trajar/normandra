package org.normandra.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import org.normandra.log.DatabaseActivity;

import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * a cassandra database listActivity, backed by a cql3 statement
 * <p>
 *  Date: 4/4/14
 */
public class CassandraDatabaseActivity implements DatabaseActivity
{
    private final Statement statement;

    private final Session session;

    private final Type type;

    private final AtomicBoolean success = new AtomicBoolean(false);

    private ResultSet results;

    private long duration = -1;

    private Date date;

    public CassandraDatabaseActivity(final Statement statement, final Session session, final Type type)
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
        if (this.statement instanceof RegularStatement)
        {
            return ((RegularStatement) this.statement).getQueryString();
        }
        else if (this.statement instanceof BoundStatement)
        {
            return ((BoundStatement) this.statement).preparedStatement().getQueryString();
        }
        else if (this.statement instanceof BatchStatement)
        {
            return ((BatchStatement) this.statement).getStatements().toString();
        }
        else
        {
            return this.statement.toString();
        }
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
