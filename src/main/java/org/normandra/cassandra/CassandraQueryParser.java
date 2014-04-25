package org.normandra.cassandra;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.normandra.DatabaseQuery;
import org.normandra.NormandraException;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * a simple jpa query parser
 * <p>
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

        final String tableQuery = prepare(this.entity, jpaQuery);
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


    public static String prepare(final EntityContext entity, final String query) throws NormandraException
    {
        if (null == query || query.isEmpty())
        {
            return "";
        }
        String result = query;
        result = replaceEntityNames(entity, result);
        result = ensureColumnNames(result);
        return result;
    }


    private static String replaceEntityNames(final EntityContext entity, final String query) throws NormandraException
    {
        String result = query;
        for (final EntityMeta meta : entity.getEntities())
        {
            final List<String> tables = new ArrayList<>(2);
            for (final TableMeta table : meta)
            {
                if (!table.isSecondary())
                {
                    tables.add(table.getName());
                }
            }
            if (tables.size() > 1)
            {
                throw new NormandraException("CQL3 queries only support a table table.");
            }
            result = result.replace(meta.getName(), tables.get(0));
        }
        return result;
    }


    private static String ensureColumnNames(final String query) throws NormandraException
    {
        final String upperCase = query.toUpperCase();
        final Matcher selectMatcher = Pattern.compile("SELECT").matcher(upperCase);
        if (!selectMatcher.find())
        {
            return query;
        }

        final Matcher fromMatcher = Pattern.compile("FROM").matcher(upperCase);
        if (!fromMatcher.find(selectMatcher.end()))
        {
            return query;
        }

        final String columns = query.substring(selectMatcher.end() + 1, fromMatcher.start()).trim();
        if (!columns.isEmpty())
        {
            return query;
        }

        final String result = query.substring(0, selectMatcher.end() + 1) + " * " + query.substring(fromMatcher.start());
        return result;
    }
}
