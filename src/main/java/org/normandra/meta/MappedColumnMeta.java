package org.normandra.meta;

/**
 * a column that is mapped to another table, which ultimately owns the data
 * <p>
 * 
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
    public boolean isEmbedded()
    {
        return false;
    }
}
