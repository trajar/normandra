package org.normandra.generator;

import org.normandra.EntitySession;
import org.normandra.meta.EntityMeta;

import java.util.UUID;

/**
 * a simple uuid generator
 * <p>
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
    public UUID generate(EntitySession session, EntityMeta entity)
    {
        return UUID.randomUUID();
    }
}
