package org.normandra;

/**
 * an interface for handling transactional exceptions
 *
 * @date 12/4/20.
 */
public interface ExceptionHandler {
    default boolean needsRetry(Exception error) { return false; }
    default void handleError(Exception error) { }
}
