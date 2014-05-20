package org.normandra.entities;

import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.TableGenerator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * simple entity exemplar
 * <p/>
 * User: bowen
 * Date: 9/1/13
 */
@Entity
public class SimpleEntity
{
    @Id
    @TableGenerator(name = "id_gen", table = "guid")
    @GeneratedValue(generator = "id_gen", strategy = GenerationType.TABLE)
    private long id;

    @Column(name = "name_column")
    private String name;

    @ElementCollection
    private Collection<String> values;


    @Deprecated
    public SimpleEntity()
    {

    }


    public SimpleEntity(final String name, final Collection<String> c)
    {
        this.name = name;
        this.values = new ArrayList<>(c);
    }


    public long getId()
    {
        return id;
    }


    public String getName()
    {
        return name;
    }


    public Collection<String> getValues()
    {
        return Collections.unmodifiableCollection(this.values);
    }
}
