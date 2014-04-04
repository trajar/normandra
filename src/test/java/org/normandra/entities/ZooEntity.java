package org.normandra.entities;

import org.normandra.util.ArraySet;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

/**
 * User: bowen
 * Date: 3/30/14
 */
@Entity
public class ZooEntity
{
    @OneToMany
    private Set<AnimalEntity> animals;

    @Id
    private UUID id;


    public ZooEntity()
    {

    }


    public ZooEntity(final Collection<? extends AnimalEntity> c)
    {
        this.animals = new ArraySet<>(c);
    }


    public Set<AnimalEntity> getAnimals()
    {
        if (null == this.animals)
        {
            return Collections.emptySet();
        }
        else
        {
            return Collections.unmodifiableSet(this.animals);
        }
    }


    public UUID getId()
    {
        return id;
    }
}
