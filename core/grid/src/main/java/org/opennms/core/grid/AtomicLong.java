package org.opennms.core.grid;

public interface AtomicLong {
    /**
     * Atomically adds the given value to the current value.
     *
     * @param delta the value to add
     * @return the updated value
     */
    public long addAndGet(long delta);

    /**
     * Atomically sets the value to the given updated value
     * only if the current value {@code ==} the expected value.
     *
     * @param expect the expected value
     * @param update the new value
     * @return true if successful; or false if the actual value
     *         was not equal to the expected value.
     */
    public boolean compareAndSet(long expect, long update);

    /**
     * Atomically decrements the current value by one.
     *
     * @return the updated value
     */
    public long decrementAndGet();

    /**
     * Gets the current value.
     *
     * @return the current value
     */
    public long get();

    /**
     * Atomically adds the given value to the current value.
     *
     * @param delta the value to add
     * @return the old value before the add
     */
    public long getAndAdd(long delta);

    /**
     * Atomically sets the given value and returns the old value.
     *
     * @param newValue the new value
     * @return the old value
     */
    public long getAndSet(long newValue);

    /**
     * Atomically increments the current value by one.
     *
     * @return the updated value
     */
    public long incrementAndGet();

    /**
     * Atomically increments the current value by one.
     *
     * @return the old value
     */
    public long getAndIncrement();

    /**
     * Atomically sets the given value.
     *
     * @param newValue the new value
     */
    public void set(long newValue);
}
