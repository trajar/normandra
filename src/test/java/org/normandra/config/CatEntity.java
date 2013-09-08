package org.normandra.config;

import javax.persistence.Column;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

/**
 * cat entity class
 * <p/>
 * User: bowen
 * Date: 9/1/13
 */
@Entity
@DiscriminatorValue("cat")
public class CatEntity extends AnimalEntity
{
    @Column
    private boolean litterBox;
}
