package com.lambdaworks.redis.concurrent;


import java.util.concurrent.Future;

/**
 * User: roger
 * Date: 14-1-2 下午7:01
 */
public interface Promise<T> extends Future<T> {
    public Promise<T> then(Callback<T> callback);
    public Promise<T> then(Callback<T> callback, FailCallback failCallback);
    public Promise<T> fail(FailCallback failCallback);


}
