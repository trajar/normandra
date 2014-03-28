package org.normandra.entities;

import org.normandra.util.ArraySet;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.util.Collection;
import java.util.Collections;

/**
 * class entity
 * <p>
 * User: bowen
 * Date: 2/14/14
 */
@Table(name = "classroom")
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

    @JoinTable(name = "classroom_student_xref")
    @JoinColumn(name = "student_id")
    @OneToMany(fetch = FetchType.LAZY)
    private Collection<StudentEntity> students;


    public ClassEntity()
    {

    }


    public ClassEntity(final String name, final int room)
    {
        this.name = name;
        this.room = room;
    }


    public Collection<StudentEntity> getStudents()
    {
        if (null == this.students)
        {
            return Collections.emptyList();
        }
        else
        {
            return Collections.unmodifiableCollection(this.students);
        }
    }


    public boolean addStudent(final StudentEntity entity)
    {
        if (null == this.students)
        {
            this.students = new ArraySet<>();
        }
        return this.students.add(entity);
    }


    public boolean removeStudent(final StudentEntity entity)
    {
        if (null == this.students)
        {
            return false;
        }
        else
        {
            return this.students.remove(entity);
        }
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
        if (students != null ? !students.equals(that.students) : that.students != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + room;
        result = 31 * result + (students != null ? students.hashCode() : 0);
        return result;
    }
}
