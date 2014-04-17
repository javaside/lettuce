package com.lambdaworks.redis.concurrent;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: roger
 * Date: 14-1-2 下午7:41
 */
public class Promises<T> extends DeferredObject<List<T>> {
    private final int numberOfPromises;
    private final AtomicInteger doneCount = new AtomicInteger();
    private final AtomicInteger failCount = new AtomicInteger();

    private final List<T> results = new CopyOnWriteArrayList<T>();


    public Promises(Promise<T>... promises) {
        this.numberOfPromises = promises.length;

        for(final Promise<T> promise : promises) {

            promise.then(new Callback<T>() {
                @Override
                public void call(T value) {
                    results.add(value);
                    int done = doneCount.incrementAndGet();
                    if(done == numberOfPromises) {
                        resolve(results);
                    }
                }
            }).fail(new FailCallback() {

                @Override
                public void fail(String error) {
                    failCount.incrementAndGet();
                    failure(error);
                }
            });
        }
    }

    public static <T> Promise<List<T>> promises(Promise<T>... promises) {
        return new Promises<T>(promises);
    }
}
