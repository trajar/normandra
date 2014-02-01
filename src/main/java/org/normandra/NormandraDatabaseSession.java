package org.normandra;

import org.normandra.meta.EntityMeta;

import java.io.Serializable;

/**
 * a database session
 * <p/>
 * User: bowen
 * Date: 2/1/14
 */
public interface NormandraDatabaseSession
{
    void close();
    void clear() throws NormandraException;
    void beginTransaction() throws NormandraException;
    void commitTransaction() throws NormandraException;
    void rollbackTransaction() throws NormandraException;
    <T> boolean exists(EntityMeta<T> meta, Serializable key) throws NormandraException;
    <T> T get(EntityMeta<T> meta, Serializable key) throws NormandraException;
    <T> void save(EntityMeta<T> meta, T element) throws NormandraException;
    <T> void delete(EntityMeta<T> meta, T element) throws NormandraException;
}
