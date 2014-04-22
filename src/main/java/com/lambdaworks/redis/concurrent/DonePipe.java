package com.lambdaworks.redis.concurrent;

/**
 * Created by roger on 14-4-16.
 */
public interface DonePipe<T, R> {
    public Promise<R> pipeDone(T value);
}
