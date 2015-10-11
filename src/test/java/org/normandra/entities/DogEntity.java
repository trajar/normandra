package org.normandra.entities;

import javax.persistence.Column;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.NamedQuery;

/**
 * dog entity class
 * <p/>
 * 
 * Date: 9/1/13
 */
@Entity
@DiscriminatorValue("dog")
@NamedQuery(name = "dog_by_id", query = "select from DogEntity where id = :id")
public class DogEntity extends AnimalEntity
{
    @Column
    private int numBarks;


    public DogEntity()
    {

    }


    public DogEntity(final String name, final int barks)
    {
        super(name);
        this.numBarks = barks;
    }


    public int getNumBarks()
    {
        return numBarks;
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        DogEntity dogEntity = (DogEntity) o;

        if (numBarks != dogEntity.numBarks) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + numBarks;
        return result;
    }
}
