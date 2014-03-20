package org.normandra.cassandra;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.normandra.NormandraException;
import org.normandra.generator.IdGenerator;
import org.normandra.meta.EntityMeta;

/**
 * a sequenced id generator
 * <p/>
 * User: bowen
 * Date: 1/26/14
 */
public class CounterIdGenerator implements IdGenerator<Long>
{
    private static final Object lock = new Object();

    private final String tableName;

    private final String keyColumn;

    private final String valueColumn;

    private final String keyValue;

    private final SessionAccessor sessionAccessor;


    protected CounterIdGenerator(final String table, final String keyCol, final String valueCol, final String key, final SessionAccessor accessor)
    {
        this.tableName = table;
        this.keyColumn = keyCol;
        this.valueColumn = valueCol;
        this.keyValue = key;
        this.sessionAccessor = accessor;
    }


    @Override
    public Long generate(final EntityMeta entity) throws NormandraException
    {
        if (null == entity)
        {
            return null;
        }

        synchronized (lock)
        {
            try
            {
                // try to update counter a fixed number of times
                for (int i = 0; i < 10; i++)
                {
                    final Long value = this.incrementCounter();
                    if (value != null)
                    {
                        return value;
                    }
                }
            }
            catch (final Exception e)
            {
                throw new NormandraException("Unable to increment counter id [" + this.keyValue + "] from table [" + this.tableName + "].", e);
            }
        }

        throw new NormandraException("Unable to generate counter id.");
    }


    private Long incrementCounter()
    {
        final String keyspace = this.sessionAccessor.getKeyspace();
        final Session session = this.sessionAccessor.getSession();

        final RegularStatement increment = QueryBuilder.update(keyspace, this.tableName)
                .with(QueryBuilder.incr(this.valueColumn))
                .where(QueryBuilder.eq(this.keyColumn, this.keyValue));

        final RegularStatement select = QueryBuilder.select(this.valueColumn)
                .from(keyspace, this.tableName)
                .where(QueryBuilder.eq(this.keyColumn, this.keyValue));

        ResultSet results = session.execute(select);
        Row row = results != null ? results.one() : null;
        final Long current = row != null ? row.getLong(0) : null;

        session.execute(increment);

        results = session.execute(select);
        row = results != null ? results.one() : null;
        final Long next = row != null ? row.getLong(0) : null;

        if (null == next)
        {
            return null;
        }
        if (null == current && next.equals(1L))
        {
            // as expected - generated the first id
            return next;
        }
        if (current != null && next.equals(current.longValue() + 1))
        {
            // as expected - generated the next id in sequence
            return next;
        }
        // something went wrong - we generated an out of sequence or unexpected id
        return null;
    }
}
