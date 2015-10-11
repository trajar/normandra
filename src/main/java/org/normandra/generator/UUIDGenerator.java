package org.normandra.generator;

import org.normandra.meta.EntityMeta;

import java.util.UUID;

/**
 * a simple uuid generator
 * 
 * Date: 3/21/14
 */
public class UUIDGenerator implements IdGenerator
{
    private static final UUIDGenerator instance = new UUIDGenerator();


    public static IdGenerator getInstance()
    {
        return UUIDGenerator.instance;
    }


    private UUIDGenerator()
    {

    }


    @Override
    public UUID generate(EntityMeta entity)
    {
        return UUID.randomUUID();
    }
}
