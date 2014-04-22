package com.lambdaworks.redis.concurrent;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by roger on 14-4-16.
 */
public abstract class AbstractPromise<T> implements Promise<T> {
    private static ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    protected final List<Callback<T>> doneCallbacks = new CopyOnWriteArrayList<Callback<T>>();
    protected final List<FailCallback> failCallbacks = new CopyOnWriteArrayList<FailCallback>();
    protected T value;
    protected String error;
    protected volatile Promise.State state = Promise.State.PENDING;

    private boolean hasError() {
        return error != null;
    }

    public boolean isPending() {
        return state == Promise.State.PENDING;
    }

    private boolean isDone() {
        return state == Promise.State.RESOLVED;
    }
    @Override
    public Promise<T> then(Callback<T> callback) {
        doneCallbacks.add(callback);
        if(isDone()) {
            triggerDone(value);
        }
        return this;
    }

    @Override
    public Promise<T> fail(FailCallback failCallback) {
        failCallbacks.add(failCallback);
        if(hasError()) {
            triggerError(error);
        }

        return this;
    }

    @Override
    public Promise<T> then(Callback<T> callback, FailCallback failCallback) {
        then(callback);
        fail(failCallback);
        return this;
    }

    protected void triggerDone(final T resolved) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                for (Callback<T> callback : doneCallbacks) {
                    try {
                        callback.call(resolved);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });

    }


    protected void triggerError(final String error) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                for(FailCallback failCallback : failCallbacks) {
                    failCallback.fail(error);
                }
            }
        });

    }

    @Override
    public Promise<T> then(DonePipe<T> pipeCallback) {
        return new PipedPromise<T>(this, pipeCallback);
    }
}
