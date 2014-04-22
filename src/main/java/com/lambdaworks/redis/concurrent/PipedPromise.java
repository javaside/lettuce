package com.lambdaworks.redis.concurrent;

/**
 * Created by roger on 14-4-16.
 */
public class PipedPromise<T, R> extends DeferredObject<R> {
    public PipedPromise(final Promise<T> promise,final DonePipe<T, R> donePipe) {
        promise.then(new Callback<T>() {
            @Override
            public void call(T value) {
                if(donePipe != null) pipe(donePipe.pipeDone(value));
                else PipedPromise.this.resolve(null);
            }
        });
    }

    private Promise<R> pipe(Promise<R> promise) {
        if (promise != null) {
            promise.then(new Callback<R>() {
                @Override
                public void call(R value) {
                    PipedPromise.this.resolve(value);
                }
            });
        }

        return promise;
    }

}
