package org.normandra.orientdb;


/**
 * orientdb prepared statement context
 * <p/>
 * User: bowen
 * Date: 06/08/2014
 */
public class OrientQuery
{
    private final String jpaQuery;

    private final String orientQuery;


    public OrientQuery(final String jpa, final String orient)
    {
        this.jpaQuery = jpa;
        this.orientQuery = orient;
    }


    public String getQuery()
    {
        return this.orientQuery;
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

        OrientQuery that = (OrientQuery) o;

        if (jpaQuery != null ? !jpaQuery.equals(that.jpaQuery) : that.jpaQuery != null) return false;
        if (orientQuery != null ? !orientQuery.equals(that.orientQuery) : that.orientQuery != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        int result = jpaQuery != null ? jpaQuery.hashCode() : 0;
        result = 31 * result + (orientQuery != null ? orientQuery.hashCode() : 0);
        return result;
    }
}
