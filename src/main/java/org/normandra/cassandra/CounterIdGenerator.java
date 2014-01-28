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

        try
        {
            return this.incrementCounter();
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to increment counter id [" + this.keyValue + "] from table [" + this.tableName + "].", e);
        }
    }


    private Long incrementCounter()
    {
        final String keyspace = this.sessionAccessor.getKeyspace();

        final RegularStatement increment = QueryBuilder.update(keyspace, this.tableName)
                .with(QueryBuilder.incr(this.valueColumn))
                .where(QueryBuilder.eq(this.keyColumn, this.keyValue));

        final RegularStatement select = QueryBuilder.select(this.valueColumn)
                .from(keyspace, this.tableName)
                .where(QueryBuilder.eq(this.keyColumn, this.keyValue));

        final Session session = this.sessionAccessor.getSession();
        session.execute(increment);
        final ResultSet results = session.execute(select);
        if (null == results)
        {
            return null;
        }
        final Row row = results.one();
        if (null == row)
        {
            return null;
        }
        return row.getLong(0);
    }
}
