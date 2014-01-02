package com.lambdaworks.redis.concurrent;

/**
 * User: roger
 * Date: 14-1-2 下午7:01
 */
public interface Callback<T> {
    public void call(T value);

}
