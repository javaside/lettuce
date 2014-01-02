// Copyright (C) 2011 - Will Glozer.  All rights reserved.

package com.lambdaworks.redis.protocol;

import com.lambdaworks.redis.RedisCommandInterruptedException;
import com.lambdaworks.redis.concurrent.Callback;
import com.lambdaworks.redis.concurrent.FailCallback;
import com.lambdaworks.redis.concurrent.Promise;
import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.concurrent.*;

/**
 * A redis command and its result. All successfully executed commands will
 * eventually return a {@link CommandOutput} object.
 *
 * @param <T> Command output type.
 *
 * @author Will Glozer
 */
public class Command<K, V, T> implements Promise<T> {
    private static final byte[] CRLF = "\r\n".getBytes(Charsets.ASCII);

    public final CommandType type;
    protected CommandArgs<K, V> args;
    protected CommandOutput<K, V, T> output;
    protected CountDownLatch latch;
    protected final List<Callback<T>> doneCallbacks = new CopyOnWriteArrayList<Callback<T>>();
    protected final List<FailCallback> failCallbacks = new CopyOnWriteArrayList<FailCallback>();
    /**
     * Create a new command with the supplied type and args.
     *
     * @param type      Command type.
     * @param output    Command output.
     * @param args      Command args, if any.
     * @param multi     Flag indicating if MULTI active.
     */
    public Command(CommandType type, CommandOutput<K, V, T> output, CommandArgs<K, V> args, boolean multi) {
        this.type   = type;
        this.output = output;
        this.args   = args;
        this.latch  = new CountDownLatch(multi ? 2 : 1);
    }

    /**
     * Cancel the command and notify any waiting consumers. This does
     * not cause the redis server to stop executing the command.
     *
     * @param ignored Ignored parameter.
     *
     * @return true if the command was cancelled.
     */
    @Override
    public boolean cancel(boolean ignored) {
        boolean cancelled = false;
        if (latch.getCount() == 1) {
            latch.countDown();
            output = null;
            cancelled = true;
        }
        return cancelled;
    }

    /**
     * Check if the command has been cancelled.
     *
     * @return True if the command was cancelled.
     */
    @Override
    public boolean isCancelled() {
        return latch.getCount() == 0 && output == null;
    }

    /**
     * Check if the command has completed.
     *
     * @return true if the command has completed.
     */
    @Override
    public boolean isDone() {
        return latch.getCount() == 0;
    }

    /**
     * Get the command output and if the command hasn't completed
     * yet, wait until it does.
     *
     * @return The command output.
     */
    @Override
    public T get() {
        try {
            latch.await();
            return output.get();
        } catch (InterruptedException e) {
            throw new RedisCommandInterruptedException(e);
        }
    }

    /**
     * Get the command output and if the command hasn't completed yet,
     * wait up to the specified time until it does.
     *
     * @param timeout   Maximum time to wait for a result.
     * @param unit      Unit of time for the timeout.
     *
     * @return The command output.
     *
     * @throws TimeoutException if the wait timed out.
     */
    @Override
    public T get(long timeout, TimeUnit unit) throws TimeoutException {
        try {
            if (!latch.await(timeout, unit)) {
                throw new TimeoutException("Command timed out");
            }
        } catch (InterruptedException e) {
            throw new RedisCommandInterruptedException(e);
        }
        return output.get();
    }

    /**
     * Wait up to the specified time for the command output to become
     * available.
     *
     * @param timeout   Maximum time to wait for a result.
     * @param unit      Unit of time for the timeout.
     *
     * @return true if the output became available.
     */
    public boolean await(long timeout, TimeUnit unit) {
        try {
            return latch.await(timeout, unit);
        } catch (InterruptedException e) {
            throw new RedisCommandInterruptedException(e);
        }
    }

    /**
     * Get the object that holds this command's output.
     *
     * @return  The command output object.
     */
    public CommandOutput<K, V, T> getOutput() {
        return output;
    }

    /**
     * Mark this command complete and notify all waiting threads.
     */
    public void complete() {
        latch.countDown();
        if(output.hasError()) {
            triggerError(output.getError());
        } else {
            triggerDone(output.get());
        }


    }

    /**
     * Encode and write this command to the supplied buffer using the new
     * <a href="http://redis.io/topics/protocol">Unified Request Protocol</a>.
     *
     * @param buf Buffer to write to.
     */
    void encode(ByteBuf buf) {
        buf.writeByte('*');
        writeInt(buf, 1 + (args != null ? args.count() : 0));
        buf.writeBytes(CRLF);
        buf.writeByte('$');
        writeInt(buf, type.bytes.length);
        buf.writeBytes(CRLF);
        buf.writeBytes(type.bytes);
        buf.writeBytes(CRLF);
        if (args != null) {
            buf.writeBytes(args.buffer());
        }
    }

    /**
     * Write the textual value of a positive integer to the supplied buffer.
     *
     * @param buf   Buffer to write to.
     * @param value Value to write.
     */
    protected static void writeInt(ByteBuf buf, int value) {
        if (value < 10) {
            buf.writeByte('0' + value);
            return;
        }

        StringBuilder sb = new StringBuilder(8);
        while (value > 0) {
            int digit = value % 10;
            sb.append((char) ('0' + digit));
            value /= 10;
        }

        for (int i = sb.length() - 1; i >= 0; i--) {
            buf.writeByte(sb.charAt(i));
        }
    }

    @Override
    public Promise<T> then(Callback<T> callback) {
        doneCallbacks.add(callback);
        if(isDone()) {
            triggerDone(output.get());
        }
        return this;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Promise<T> fail(FailCallback failCallback) {
        failCallbacks.add(failCallback);
        if(isDone() && output.hasError()) {
            triggerError(output.getError());
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
}
