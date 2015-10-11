package org.normandra.entities;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * index of students
 * <p/>
 * 
 * Date: 3/2/14
 */
@Table(name = "student_index")
@Entity
public class StudentIndexEntity
{
    @Id
    private String name;

    @Id
    private Long classroomId;


    public StudentIndexEntity()
    {

    }


    public StudentIndexEntity(final String name, final long classroomId)
    {
        this.name = name;
        this.classroomId = classroomId;
    }


    public String getName()
    {
        return name;
    }


    public Long getClassroomId()
    {
        return classroomId;
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StudentIndexEntity that = (StudentIndexEntity) o;

        if (classroomId != null ? !classroomId.equals(that.classroomId) : that.classroomId != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (classroomId != null ? classroomId.hashCode() : 0);
        return result;
    }
}
