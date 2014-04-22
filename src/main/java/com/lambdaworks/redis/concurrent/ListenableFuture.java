package com.lambdaworks.redis.concurrent;

import java.util.concurrent.Future;

/**
 * Created by roger on 14-4-16.
 */
public interface ListenableFuture<T> extends Promise<T>, Future<T> {

}
