package com.lambdaworks.redis.concurrent;

import java.util.List;
import java.util.concurrent.*;

/**
 * Created by roger on 14-4-16.
 */
public class DeferredObject<T> extends AbstractPromise<T> {
    public void resolve(T value) {
        this.value = value;
        this.state = Promise.State.RESOLVED;
        triggerDone(value);
    }

    public void failure(String error) {
        this.error = error;
        this.state = Promise.State.FAILURE;
        triggerError(error);
    }


}
