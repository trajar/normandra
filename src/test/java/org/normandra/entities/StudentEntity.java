package org.normandra.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import java.util.UUID;

/**
 * generic student entity
 * <p/>
 * User: bowen
 * Date: 2/14/14
 */
@Entity
public class StudentEntity
{
    @Id
    private UUID id = UUID.randomUUID();

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
}
