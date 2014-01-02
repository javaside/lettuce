package com.lambdaworks.redis.concurrent;

/**
 * User: roger
 * Date: 14-1-2 下午7:09
 */
public interface FailCallback {
    public void fail(String error);
}
