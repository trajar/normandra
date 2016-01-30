package org.normandra.util;

import org.normandra.NormandraException;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * query utility methods
 * <p>
 * <p>
 * Date: 6/9/14
 */
public class QueryUtils
{
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
        if (null == entity)
        {
            return query;
        }

        String result = query;
        for (final EntityMeta meta : entity.getEntities())
        {
            final List<String> tables = new ArrayList<>(2);
            for (final TableMeta table : meta)
            {
                if (!table.isJoinTable())
                {
                    tables.add(table.getName());
                }
            }
            if (tables.size() > 1)
            {
                throw new NormandraException("CQL3 queries only support a table table.");
            }
            final String table = tables.get(0);
            result = result.replace(meta.getName(), table);
            Class<?> parent = meta.getType().getSuperclass();
            while (parent != null && !Object.class.equals(parent))
            {
                result = result.replace(parent.getSimpleName(), table);
                parent = parent.getSuperclass();
            }
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

    private QueryUtils()
    {

    }
}
