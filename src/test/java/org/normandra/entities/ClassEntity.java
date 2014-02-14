package org.normandra.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

/**
 * class entity
 * <p/>
 * User: bowen
 * Date: 2/14/14
 */
@Entity
public class ClassEntity
{
    @Id
    @GeneratedValue(strategy = GenerationType.TABLE)
    private Long id;

    @Column
    private String name;

    @Column
    private int room;


    public ClassEntity()
    {

    }


    public ClassEntity(final String name, final int room)
    {
        this.name = name;
        this.room = room;
    }


    public Long getId()
    {
        return id;
    }


    public String getName()
    {
        return name;
    }


    public int getRoom()
    {
        return room;
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClassEntity that = (ClassEntity) o;

        if (room != that.room) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + room;
        return result;
    }
}
