package com.flashexchange.engine.matching;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Single-threaded free-list pool. Pre-allocates {@code initialSize} objects so the steady state is allocation free;
 * when exhausted it allocates new objects (counted by {@link #allocationsBeyondInitial()}) rather than failing.
 *
 * @param <T> pooled type.
 */
public final class ObjectPool<T>
{
    private final Supplier<T> factory;
    private final Consumer<T> resetter;
    private Object[] free;
    private int size;
    private long allocationsBeyondInitial;

    /**
     * Create a pool.
     *
     * @param initialSize number of objects to pre-allocate (at least 1).
     * @param factory     creating new objects.
     * @param resetter    clearing an object when it is released.
     */
    public ObjectPool(final int initialSize, final Supplier<T> factory, final Consumer<T> resetter)
    {
        if (initialSize < 1)
        {
            throw new IllegalArgumentException("initialSize must be >= 1: " + initialSize);
        }
        this.factory = factory;
        this.resetter = resetter;
        this.free = new Object[initialSize];
        for (int i = 0; i < initialSize; i++)
        {
            free[i] = factory.get();
        }
        this.size = initialSize;
    }

    /**
     * Take an object from the pool.
     *
     * @return a reset object.
     */
    @SuppressWarnings("unchecked")
    public T acquire()
    {
        if (size == 0)
        {
            allocationsBeyondInitial++;
            return factory.get();
        }
        final T value = (T)free[--size];
        free[size] = null;
        return value;
    }

    /**
     * Return an object to the pool. It is reset immediately.
     *
     * @param value to release.
     */
    public void release(final T value)
    {
        resetter.accept(value);
        if (size == free.length)
        {
            free = Arrays.copyOf(free, free.length * 2);
        }
        free[size++] = value;
    }

    /**
     * Number of objects available without allocation.
     *
     * @return free count.
     */
    public int available()
    {
        return size;
    }

    /**
     * Number of objects allocated because the pool was empty.
     *
     * @return allocation count.
     */
    public long allocationsBeyondInitial()
    {
        return allocationsBeyondInitial;
    }
}
