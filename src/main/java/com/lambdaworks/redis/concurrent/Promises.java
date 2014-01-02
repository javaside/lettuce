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
public class Promises<T> implements Promise<List<T>> {
    private final int numberOfPromises;
    private final AtomicInteger doneCount = new AtomicInteger();
    private final AtomicInteger failCount = new AtomicInteger();
    private final List<Callback<List<T>>> doneCallbacks = new CopyOnWriteArrayList<Callback<List<T>>>();
    private final List<FailCallback> failCallbacks = new CopyOnWriteArrayList<FailCallback>();
    private final List<String>  errors = new CopyOnWriteArrayList<String>();
    private final List<T> results = new CopyOnWriteArrayList<T>();
    private final List<Promise<T>> promises = new ArrayList<Promise<T>>();

    public Promises(Promise<T>... promises) {
        this.numberOfPromises = promises.length;

        for(final Promise<T> promise : promises) {
            this.promises.add(promise);
            promise.then(new Callback<T>() {
                @Override
                public void call(T value) {
                    results.add(value);
                    int done = doneCount.incrementAndGet();
                    if(done == numberOfPromises) {
                        triggerDone();
                    }
                }
            }).fail(new FailCallback() {

                @Override
                public void fail(String error) {
                    failCount.incrementAndGet();
                    errors.add(error);
                    triggerFail();
                }
            });
        }
    }

    private void triggerDone() {
        for(Callback<List<T>> callback : doneCallbacks) {
            callback.call(results);
        }

    }

    private void triggerFail() {
        StringBuilder sb = new StringBuilder();
        for(String error : errors) {
            sb.append(error).append("\r\n");
        }
        for(FailCallback failCallback : failCallbacks) {
            failCallback.fail(sb.toString());
        }
    }

    @Override
    public Promise<List<T>> then(Callback<List<T>> callback) {
        doneCallbacks.add(callback);
        if(doneCount.get() == numberOfPromises) {
            triggerDone();
        }
        return this;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Promise<List<T>> then(Callback<List<T>> callback, FailCallback failCallback) {
        doneCallbacks.add(callback);
        fail(failCallback);
        return this;
    }

    @Override
    public Promise<List<T>> fail(FailCallback failCallback) {
        failCallbacks.add(failCallback);
        if(failCount.get() > 0) {
            triggerFail();
        }
        return this;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCancelled() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDone() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<T> get() throws InterruptedException, ExecutionException {
        List<T> values = new ArrayList<T>();
        for(Promise<T> promise : promises) {
            values.add(promise.get());

        }
        return values;
    }

    @Override
    public List<T> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        List<T> values = new ArrayList<T>();
        for(Promise<T> promise : promises) {
            values.add(promise.get(timeout, unit));
        }
        return values;
    }


    public static <T> Promise<List<T>> promises(Promise<T>... promises) {
        return new Promises<T>(promises);
    }
}
