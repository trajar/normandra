package org.normandra.config;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Collection;

/**
 * simple entity exemplar
 * <p/>
 * User: bowen
 * Date: 9/1/13
 */
@Table
@Entity
public class SimpleEntity
{
    @Id
    private long id;

    @Column(name = "name_colum")
    private String name;

    @Column
    private Collection<String> values;
}
