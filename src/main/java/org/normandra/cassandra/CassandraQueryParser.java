package org.normandra.cassandra;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.normandra.DatabaseQuery;
import org.normandra.NormandraException;
import org.normandra.meta.EntityContext;
import org.normandra.util.QueryUtils;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * a simple jpa query parser
 * <p/>
 * User: bowen
 * Date: 4/20/14
 */
public class CassandraQueryParser<T>
{
    private final CassandraDatabaseSession session;

    private final EntityContext entity;


    public CassandraQueryParser(final EntityContext entity, final CassandraDatabaseSession session)
    {
        this.entity = entity;
        this.session = session;
    }


    public DatabaseQuery<T> parse(final String jpaQuery, final Map<String, Object> parameters) throws NormandraException
    {
        if (null == jpaQuery || jpaQuery.isEmpty())
        {
            return null;
        }

        final String tableQuery = QueryUtils.prepare(this.entity, jpaQuery);
        if (parameters.isEmpty())
        {
            final RegularStatement statement = new SimpleStatement(tableQuery);
            return new CassandraDatabaseQuery<>(this.entity, statement, this.session);
        }

        try
        {
            final StringBuilder buffer = new StringBuilder();
            final Matcher matcher = Pattern.compile(":\\w+").matcher(tableQuery);
            int last = 0;
            while (matcher.find())
            {
                final int start = matcher.start();
                final int end = matcher.end();
                last = end;
                if (buffer.length() <= 0)
                {
                    buffer.append(tableQuery.substring(0, start));
                }
                String key = matcher.group();
                key = key.substring(1);
                final Object value = parameters.get(key);
                if (value != null)
                {
                    buffer.append(QueryBuilder.raw(value.toString()));
                }
                else
                {
                    buffer.append("null");
                }
            }
            if (last > 0 && last < tableQuery.length() - 1)
            {
                buffer.append(tableQuery.substring(last + 1));
            }
            final RegularStatement statement = new SimpleStatement(buffer.toString());
            return new CassandraDatabaseQuery<>(this.entity, statement, this.session);
        }
        catch (final PatternSyntaxException e)
        {
            throw new NormandraException("Unable to parse query.", e);
        }
    }
}
