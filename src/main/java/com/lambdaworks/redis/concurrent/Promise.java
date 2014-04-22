package com.lambdaworks.redis.concurrent;


/**
 * User: roger
 * Date: 14-1-2 下午7:01
 */
public interface Promise<T> {
    public enum State {
        PENDING,
        FAILURE,
        RESOLVED
    }

    public Promise<T> then(Callback<T> callback);
    public Promise<T> then(Callback<T> callback, FailCallback failCallback);
    public Promise<T> fail(FailCallback failCallback);
    public <R> Promise<R> then(DonePipe<T, R> pipeCallback);


}
