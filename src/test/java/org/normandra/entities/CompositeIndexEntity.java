package org.normandra.entities;

import javax.persistence.Embeddable;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.UUID;

/**
 * index of students
 * <p/>
 * User: bowen
 * Date: 3/2/14
 */
@Table(name = "composite_index")
@Entity
public class CompositeIndexEntity
{
    @Embeddable
    public static class Key implements Serializable
    {
        @Id
        private UUID id;

        @Id
        private String name;


        public Key()
        {

        }


        public Key(final String name)
        {
            this.id = UUID.randomUUID();
            this.name = name;
        }


        public Key(final UUID id, final String name)
        {
            this.id = id;
            this.name = name;
        }
    }

    @EmbeddedId
    private Key key;


    public CompositeIndexEntity()
    {

    }


    public CompositeIndexEntity(final String name)
    {
        this.key = new Key(name);
    }


    public UUID getId()
    {
        return this.key.id;
    }


    public String getName()
    {
        return this.key.name;
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompositeIndexEntity that = (CompositeIndexEntity) o;

        if (key != null ? !key.equals(that.key) : that.key != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        return key != null ? key.hashCode() : 0;
    }
}
