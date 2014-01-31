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
}
