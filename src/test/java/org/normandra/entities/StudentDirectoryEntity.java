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
 * <p/>
 * User: bowen
 * Date: 2/14/14
 */
@Table(name = "student_directory")
@Entity
public class StudentDirectoryEntity
{
    @Id
    @GeneratedValue(strategy = GenerationType.TABLE)
    private Long id;

    @Column
    private String name;

    @JoinTable(name = "student_directory_xref")
    @JoinColumn(name = "student_id")
    @OneToMany(fetch = FetchType.LAZY)
    private Collection<StudentEntity> students;


    public StudentDirectoryEntity()
    {

    }


    public StudentDirectoryEntity(final String name)
    {
        this.name = name;
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


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StudentDirectoryEntity that = (StudentDirectoryEntity) o;

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
