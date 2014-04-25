package org.normandra.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * cassandra prepared statement context
 * <p>
 * User: bowen
 * Date: 4/24/14
 */
public class CassandraPreparedStatement
{
    private final PreparedStatement statement;

    private final String jpaQuery;

    private final List<String> fields;


    public CassandraPreparedStatement(final PreparedStatement statement, final String jpa, final Collection<String> c)
    {
        this.statement = statement;
        this.jpaQuery = jpa;
        this.fields = new ArrayList<>(c);
    }


    public BoundStatement bind(final Map<String, Object> parameters)
    {
        if (null == parameters || parameters.isEmpty())
        {
            return this.statement.bind();
        }

        final List<Object> list = new ArrayList<>(this.fields.size());
        for (final String parameter : this.fields)
        {
            final Object value = parameters.get(parameter);
            if (value != null)
            {
                list.add(value);
            }
            else
            {
                list.add(null);
            }
        }
        return this.statement.bind(list.toArray());
    }


    @Override
    public String toString()
    {
        return this.jpaQuery;
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CassandraPreparedStatement that = (CassandraPreparedStatement) o;

        if (fields != null ? !fields.equals(that.fields) : that.fields != null) return false;
        if (jpaQuery != null ? !jpaQuery.equals(that.jpaQuery) : that.jpaQuery != null) return false;
        if (statement != null ? !statement.equals(that.statement) : that.statement != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        int result = statement != null ? statement.hashCode() : 0;
        result = 31 * result + (jpaQuery != null ? jpaQuery.hashCode() : 0);
        result = 31 * result + (fields != null ? fields.hashCode() : 0);
        return result;
    }
}
