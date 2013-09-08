package org.normandra.config;

import javax.persistence.Column;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

/**
 * dog entity class
 * <p/>
 * User: bowen
 * Date: 9/1/13
 */
@Entity
@DiscriminatorValue("dog")
public class DogEntity extends AnimalEntity
{
    @Column
    private int numBarks;
}
