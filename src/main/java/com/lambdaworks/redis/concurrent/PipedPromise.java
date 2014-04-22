package com.lambdaworks.redis.concurrent;

/**
 * Created by roger on 14-4-16.
 */
public class PipedPromise<T> extends DeferredObject<T> {
    public PipedPromise(final Promise<T> promise,final DonePipe<T> donePipe) {
        promise.then(new Callback<T>() {
            @Override
            public void call(T value) {
                if(donePipe != null) pipe(donePipe.pipeDone(value));
                else PipedPromise.this.resolve(value);
            }
        });
    }

    private Promise<T> pipe(Promise<T> promise) {
        promise.then(new Callback<T>() {
            @Override
            public void call(T value) {
                PipedPromise.this.resolve(value);
            }
        });

        return promise;
    }

}
