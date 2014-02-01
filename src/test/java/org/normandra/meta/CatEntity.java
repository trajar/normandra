package org.normandra.meta;

import javax.persistence.Column;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

/**
 * cat entity class
 * <p/>
 * User: bowen
 * Date: 9/1/13
 */
@Entity
@DiscriminatorValue("cat")
public class CatEntity extends AnimalEntity
{
    @Column
    private boolean litterBox;


    public CatEntity()
    {

    }


    public CatEntity(final String name, final boolean litter)
    {
        super(name);
        this.litterBox = litter;
    }


    public boolean isLitterBox()
    {
        return litterBox;
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        CatEntity catEntity = (CatEntity) o;

        if (litterBox != catEntity.litterBox) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (litterBox ? 1 : 0);
        return result;
    }
}
