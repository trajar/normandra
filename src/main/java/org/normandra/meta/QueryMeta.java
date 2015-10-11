package org.normandra.meta;

/**
 * a jpa query annotation meta
 * <p>
 * 
 * Date: 4/26/14
 */
public class QueryMeta implements Comparable<QueryMeta>
{
    private final Class<?> entity;

    private final String name;

    private final String query;


    public QueryMeta(final Class<?> entity, final String name, final String query)
    {
        if (null == name || name.isEmpty())
        {
            throw new IllegalArgumentException("Name cannot be null/empty.");
        }
        if (null == query || query.isEmpty())
        {
            throw new IllegalArgumentException("Query cannot be null/empty.");
        }
        this.name = name;
        this.query = query;
        this.entity = entity;
    }


    public Class<?> getEntity()
    {
        return this.entity;
    }


    public String getName()
    {
        return name;
    }


    public String getQuery()
    {
        return query;
    }


    @Override
    public int compareTo(QueryMeta o)
    {
        return this.name.compareTo(o.name);
    }


    @Override
    public String toString()
    {
        return "{" + this.name + "} " + this.query;
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueryMeta queryMeta = (QueryMeta) o;

        if (entity != null ? !entity.equals(queryMeta.entity) : queryMeta.entity != null) return false;
        if (name != null ? !name.equals(queryMeta.name) : queryMeta.name != null) return false;
        if (query != null ? !query.equals(queryMeta.query) : queryMeta.query != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        int result = entity != null ? entity.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (query != null ? query.hashCode() : 0);
        return result;
    }
}
