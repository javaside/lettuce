package com.lambdaworks.redis.concurrent;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by roger on 14-4-16.
 */
public abstract class AbstractPromise<T> implements Promise<T> {
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
        return this;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Promise<T> fail(FailCallback failCallback) {
        failCallbacks.add(failCallback);
        if(isDone() && hasError()) {
            triggerError(error);
        }

        return this;
    }

    @Override
    public Promise<T> then(Callback<T> callback, FailCallback failCallback) {
        then(callback);
        fail(failCallback);
        return this;  //To change body of implemented methods use File | Settings | File Templates.
    }

    protected void triggerDone(T resolved) {
        for (Callback<T> callback : doneCallbacks) {
            try {
                callback.call(resolved);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    protected void triggerError(String error) {
        for(FailCallback failCallback : failCallbacks) {
            failCallback.fail(error);
        }
    }

    @Override
    public Promise<T> then(DonePipe<T> pipeCallback) {
        return new PipedPromise<T>(this, pipeCallback);
    }
}
