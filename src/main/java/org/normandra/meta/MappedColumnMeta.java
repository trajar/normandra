package org.normandra.meta;

/**
 * a column that is mapped to another table, which ultimately owns the data
 * <p/>
 * User: bowen
 * Date: 5/31/14
 */
public class MappedColumnMeta extends ColumnMeta
{
    private final EntityContext entity;

    private final TableMeta table;

    private final ColumnMeta column;


    public MappedColumnMeta(final EntityContext entity, final TableMeta table, final ColumnMeta column, final String name, final String property, final Class<?> clazz, final boolean lazy)
    {
        super(name, property, clazz, false, lazy);
        this.entity = entity;
        this.table = table;
        this.column = column;
    }


    public EntityContext getEntity()
    {
        return this.entity;
    }


    public TableMeta getTable()
    {
        return this.table;
    }


    public ColumnMeta getColumn()
    {
        return this.column;
    }


    @Override
    public boolean isVirtual()
    {
        return true;
    }

    @Override
    public boolean isEmbedded()
    {
        return false;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        MappedColumnMeta that = (MappedColumnMeta) o;

        if (column != null ? !column.equals(that.column) : that.column != null) return false;
        if (entity != null ? !entity.equals(that.entity) : that.entity != null) return false;
        if (table != null ? !table.equals(that.table) : that.table != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (entity != null ? entity.hashCode() : 0);
        result = 31 * result + (table != null ? table.hashCode() : 0);
        result = 31 * result + (column != null ? column.hashCode() : 0);
        return result;
    }
}
