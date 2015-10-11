package org.normandra.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import java.util.UUID;

/**
 * generic student entity
 * <p/>
 * 
 * Date: 2/14/14
 */
@Table(name = "student")
@Entity
public class StudentEntity
{
    @Id
    private UUID id;

    @Column
    private String name;

    @JoinColumn(name = "class_id")
    @ManyToOne(fetch = FetchType.LAZY)
    private ClassEntity classroom;


    public StudentEntity()
    {

    }


    public StudentEntity(final String name)
    {
        this.name = name;
    }


    public StudentEntity(final String name, final ClassEntity classroorm)
    {
        this.name = name;
        this.classroom = classroorm;
    }


    public UUID getId()
    {
        return this.id;
    }


    public String getName()
    {
        return name;
    }


    public void setName(String name)
    {
        this.name = name;
    }


    public ClassEntity getClassroom()
    {
        return classroom;
    }


    public void setClassroom(ClassEntity classroom)
    {
        this.classroom = classroom;
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StudentEntity that = (StudentEntity) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }
}
