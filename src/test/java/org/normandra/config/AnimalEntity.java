package org.normandra.config;

import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.Entity;
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
    private long id;

    @Column
    private String name;
}
