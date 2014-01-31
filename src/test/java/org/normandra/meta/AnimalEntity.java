package org.normandra.meta;

import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.Table;

/**
 * base entity for inheritance
 * <p/>
 * User: bowen
 * Date: 9/1/13
 */
@Entity
@Table(name = "animal")
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@DiscriminatorColumn(name = "type", discriminatorType = DiscriminatorType.STRING, length = 32)
public class AnimalEntity
{
    @Id
    @GeneratedValue(strategy = GenerationType.TABLE)
    private Long id;

    @Column
    private String name;


    public AnimalEntity()
    {

    }


    public AnimalEntity(final String name)
    {
        this.name = name;
    }


    public Long getId()
    {
        return id;
    }


    public String getName()
    {
        return name;
    }
}
