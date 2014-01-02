// Copyright (C) 2011 - Will Glozer.  All rights reserved.

package com.lambdaworks.redis;

import com.lambdaworks.codec.Base16;
import com.lambdaworks.redis.codec.RedisCodec;
import com.lambdaworks.redis.concurrent.Promise;
import com.lambdaworks.redis.output.*;
import com.lambdaworks.redis.protocol.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;

import static com.lambdaworks.redis.protocol.CommandKeyword.*;
import static com.lambdaworks.redis.protocol.CommandType.*;

/**
 * An asynchronous thread-safe connection to a redis server. Multiple threads may
 * share one {@link RedisAsyncConnection} provided they avoid blocking and transactional
 * operations such as {@link #blpop} and {@link #multi()}/{@link #exec}.
 *
 * A {@link ConnectionWatchdog} monitors each connection and reconnects
 * automatically until {@link #close} is called. All pending commands will be
 * (re)sent after successful reconnection.
 *
 * @author Will Glozer
 */
@ChannelHandler.Sharable
public class RedisAsyncConnection<K, V> extends ChannelInboundHandlerAdapter {
    protected BlockingQueue<Command<K, V, ?>> queue;
    protected RedisCodec<K, V> codec;
    protected Channel channel;
    protected long timeout;
    protected TimeUnit unit;
    protected MultiOutput<K, V> multi;
    private String password;
    private int db;
    private boolean closed;

    /**
     * Initialize a new connection.
     *
     * @param queue   Command queue.
     * @param codec   Codec used to encode/decode keys and values.
     * @param timeout Maximum time to wait for a response.
     * @param unit    Unit of time for the timeout.
     */
    public RedisAsyncConnection(BlockingQueue<Command<K, V, ?>> queue, RedisCodec<K, V> codec, long timeout, TimeUnit unit) {
        this.queue = queue;
        this.codec = codec;
        this.timeout = timeout;
        this.unit = unit;
    }

    /**
     * Set the command timeout for this connection.
     *
     * @param timeout Command timeout.
     * @param unit    Unit of time for the timeout.
     */
    public void setTimeout(long timeout, TimeUnit unit) {
        this.timeout = timeout;
        this.unit = unit;
    }

    public Promise<Long> append(K key, V value) {
        return dispatch(APPEND, new IntegerOutput<K, V>(codec), key, value);
    }

    public String auth(String password) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(password);
        Command<K, V, String> cmd = dispatch(AUTH, new StatusOutput<K, V>(codec), args);
        String status = await(cmd, timeout, unit);
        if ("OK".equals(status)) this.password = password;
        return status;
    }

    public Promise<String> bgrewriteaof() {
        return dispatch(BGREWRITEAOF, new StatusOutput<K, V>(codec));
    }

    public Promise<String> bgsave() {
        return dispatch(BGSAVE, new StatusOutput<K, V>(codec));
    }

    public Promise<Long> bitcount(K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key);
        return dispatch(BITCOUNT, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<Long> bitcount(K key, long start, long end) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(start).add(end);
        return dispatch(BITCOUNT, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<Long> bitopAnd(K destination, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.add(AND).addKey(destination).addKeys(keys);
        return dispatch(BITOP, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<Long> bitopNot(K destination, K source) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.add(NOT).addKey(destination).addKey(source);
        return dispatch(BITOP, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<Long> bitopOr(K destination, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.add(OR).addKey(destination).addKeys(keys);
        return dispatch(BITOP, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<Long> bitopXor(K destination, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.add(XOR).addKey(destination).addKeys(keys);
        return dispatch(BITOP, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<KeyValue<K, V>> blpop(long timeout, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKeys(keys).add(timeout);
        return dispatch(BLPOP, new KeyValueOutput<K, V>(codec), args);
    }

    public Promise<KeyValue<K, V>> brpop(long timeout, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKeys(keys).add(timeout);
        return dispatch(BRPOP, new KeyValueOutput<K, V>(codec), args);
    }

    public Promise<V> brpoplpush(long timeout, K source, K destination) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(source).addKey(destination).add(timeout);
        return dispatch(BRPOPLPUSH, new ValueOutput<K, V>(codec), args);
    }

    public Promise<K> clientGetname() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(GETNAME);
        return dispatch(CLIENT, new KeyOutput<K, V>(codec), args);
    }

    public Promise<String> clientSetname(K name) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(SETNAME).addKey(name);
        return dispatch(CLIENT, new StatusOutput<K, V>(codec), args);
    }

    public Promise<String> clientKill(String addr) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(KILL).add(addr);
        return dispatch(CLIENT, new StatusOutput<K, V>(codec), args);
    }

    public Promise<String> clientList() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(LIST);
        return dispatch(CLIENT, new StatusOutput<K, V>(codec), args);
    }

    public Promise<List<String>> configGet(String parameter) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(GET).add(parameter);
        return dispatch(CONFIG, new StringListOutput<K, V>(codec), args);
    }

    public Promise<String> configResetstat() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(RESETSTAT);
        return dispatch(CONFIG, new StatusOutput<K, V>(codec), args);
    }

    public Promise<String> configSet(String parameter, String value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(SET).add(parameter).add(value);
        return dispatch(CONFIG, new StatusOutput<K, V>(codec), args);
    }

    public Promise<Long> dbsize() {
        return dispatch(DBSIZE, new IntegerOutput<K, V>(codec));
    }

    public Promise<String> debugObject(K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(OBJECT).addKey(key);
        return dispatch(DEBUG, new StatusOutput<K, V>(codec), args);
    }

    public Promise<Long> decr(K key) {
        return dispatch(DECR, new IntegerOutput<K, V>(codec), key);
    }

    public Promise<Long> decrby(K key, long amount) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(amount);
        return dispatch(DECRBY, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<Long> del(K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKeys(keys);
        return dispatch(DEL, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<String> discard() {
        if (multi != null) {
            multi.cancel();
            multi = null;
        }
        return dispatch(DISCARD, new StatusOutput<K, V>(codec));
    }

    public Promise<byte[]> dump(K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key);
        return dispatch(DUMP, new ByteArrayOutput<K, V>(codec), args);
    }

    public Promise<V> echo(V msg) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addValue(msg);
        return dispatch(ECHO, new ValueOutput<K, V>(codec), args);
    }

    public <T> Promise<T> eval(V script, ScriptOutputType type, K[] keys, V... values) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addValue(script).add(keys.length).addKeys(keys).addValues(values);
        CommandOutput<K, V, T> output = newScriptOutput(codec, type);
        return dispatch(EVAL, output, args);
    }

    public <T> Promise<T> evalsha(String digest, ScriptOutputType type, K[] keys, V... values) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.add(digest).add(keys.length).addKeys(keys).addValues(values);
        CommandOutput<K, V, T> output = newScriptOutput(codec, type);
        return dispatch(EVALSHA, output, args);
    }

    public Promise<Boolean> exists(K key) {
        return dispatch(EXISTS, new BooleanOutput<K, V>(codec), key);
    }

    public Promise<Boolean> expire(K key, long seconds) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(seconds);
        return dispatch(EXPIRE, new BooleanOutput<K, V>(codec), args);
    }

    public Promise<Boolean> expireat(K key, Date timestamp) {
        return expireat(key, timestamp.getTime() / 1000);
    }

    public Promise<Boolean> expireat(K key, long timestamp) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(timestamp);
        return dispatch(EXPIREAT, new BooleanOutput<K, V>(codec), args);
    }

    public Promise<List<Object>> exec() {
        MultiOutput<K, V> multi = this.multi;
        this.multi = null;
        if (multi == null) multi = new MultiOutput<K, V>(codec);
        return dispatch(EXEC, multi);
    }

    public Promise<String> flushall() throws Exception {
        return dispatch(FLUSHALL, new StatusOutput<K, V>(codec));
    }

    public Promise<String> flushdb() throws Exception {
        return dispatch(FLUSHDB, new StatusOutput<K, V>(codec));
    }

    public Promise<V> get(K key) {
        return dispatch(GET, new ValueOutput<K, V>(codec), key);
    }

    public Promise<Long> getbit(K key, long offset) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(offset);
        return dispatch(GETBIT, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<V> getrange(K key, long start, long end) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(start).add(end);
        return dispatch(GETRANGE, new ValueOutput<K, V>(codec), args);
    }

    public Promise<V> getset(K key, V value) {
        return dispatch(GETSET, new ValueOutput<K, V>(codec), key, value);
    }

    public Promise<Long> hdel(K key, K... fields) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKeys(fields);
        return dispatch(HDEL, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<Boolean> hexists(K key, K field) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKey(field);
        return dispatch(HEXISTS, new BooleanOutput<K, V>(codec), args);
    }

    public Promise<V> hget(K key, K field) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKey(field);
        return dispatch(HGET, new ValueOutput<K, V>(codec), args);
    }

    public Promise<Long> hincrby(K key, K field, long amount) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKey(field).add(amount);
        return dispatch(HINCRBY, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<Double> hincrbyfloat(K key, K field, double amount) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKey(field).add(amount);
        return dispatch(HINCRBYFLOAT, new DoubleOutput<K, V>(codec), args);
    }

    public Promise<Map<K, V>> hgetall(K key) {
        return dispatch(HGETALL, new MapOutput<K, V>(codec), key);
    }

    public Promise<List<K>> hkeys(K key) {
        return dispatch(HKEYS, new KeyListOutput<K, V>(codec), key);
    }

    public Promise<Long> hlen(K key) {
        return dispatch(HLEN, new IntegerOutput<K, V>(codec), key);
    }

    public Promise<List<V>> hmget(K key, K... fields) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKeys(fields);
        return dispatch(HMGET, new ValueListOutput<K, V>(codec), args);
    }

    public Promise<String> hmset(K key, Map<K, V> map) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(map);
        return dispatch(HMSET, new StatusOutput<K, V>(codec), args);
    }

    public Promise<Boolean> hset(K key, K field, V value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKey(field).addValue(value);
        return dispatch(HSET, new BooleanOutput<K, V>(codec), args);
    }

    public Promise<Boolean> hsetnx(K key, K field, V value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKey(field).addValue(value);
        return dispatch(HSETNX, new BooleanOutput<K, V>(codec), args);
    }

    public Promise<List<V>> hvals(K key) {
        return dispatch(HVALS, new ValueListOutput<K, V>(codec), key);
    }

    public Promise<Long> incr(K key) {
        return dispatch(INCR, new IntegerOutput<K, V>(codec), key);
    }

    public Promise<Long> incrby(K key, long amount) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(amount);
        return dispatch(INCRBY, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<Double> incrbyfloat(K key, double amount) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(amount);
        return dispatch(INCRBYFLOAT, new DoubleOutput<K, V>(codec), args);
    }

    public Promise<String> info() {
        return dispatch(INFO, new StatusOutput<K, V>(codec));
    }

    public Promise<String> info(String section) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(section);
        return dispatch(INFO, new StatusOutput<K, V>(codec), args);
    }

    public Promise<List<K>> keys(K pattern) {
       return dispatch(KEYS, new KeyListOutput<K, V>(codec), pattern);
    }

    public Promise<Date> lastsave() {
        return dispatch(LASTSAVE, new DateOutput<K, V>(codec));
    }

    public Promise<V> lindex(K key, long index) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(index);
        return dispatch(LINDEX, new ValueOutput<K, V>(codec), args);
    }

    public Promise<Long> linsert(K key, boolean before, V pivot, V value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(before ? BEFORE : AFTER).addValue(pivot).addValue(value);
        return dispatch(LINSERT, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<Long> llen(K key) {
        return dispatch(LLEN, new IntegerOutput<K, V>(codec), key);
    }

    public Promise<V> lpop(K key) {
        return dispatch(LPOP, new ValueOutput<K, V>(codec), key);
    }

    public Promise<Long> lpush(K key, V... values) {
        return dispatch(LPUSH, new IntegerOutput<K, V>(codec), key, values);
    }

    public Promise<Long> lpushx(K key, V value) {
        return dispatch(LPUSHX, new IntegerOutput<K, V>(codec), key, value);
    }

    public Promise<List<V>> lrange(K key, long start, long stop) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(start).add(stop);
        return dispatch(LRANGE, new ValueListOutput<K, V>(codec), args);
    }

    public Promise<Long> lrem(K key, long count, V value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(count).addValue(value);
        return dispatch(LREM, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<String> lset(K key, long index, V value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(index).addValue(value);
        return dispatch(LSET, new StatusOutput<K, V>(codec), args);
    }

    public Promise<String> ltrim(K key, long start, long stop) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(start).add(stop);
        return dispatch(LTRIM, new StatusOutput<K, V>(codec), args);
    }

    public Promise<String> migrate(String host, int port, K key, int db, long timeout) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.add(host).add(port).addKey(key).add(db).add(timeout);
        return dispatch(MIGRATE, new StatusOutput<K, V>(codec), args);
    }

    public Promise<List<V>> mget(K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKeys(keys);
        return dispatch(MGET, new ValueListOutput<K, V>(codec), args);
    }

    public Promise<Boolean> move(K key, int db) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(db);
        return dispatch(MOVE, new BooleanOutput<K, V>(codec), args);
    }

    public Promise<String> multi() {
        Command<K, V, String> cmd = dispatch(MULTI, new StatusOutput<K, V>(codec));
        multi = (multi == null ? new MultiOutput<K, V>(codec) : multi);
        return cmd;
    }

    public Promise<String> mset(Map<K, V> map) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(map);
        return dispatch(MSET, new StatusOutput<K, V>(codec), args);
    }

    public Promise<Boolean> msetnx(Map<K, V> map) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(map);
        return dispatch(MSETNX, new BooleanOutput<K, V>(codec), args);
    }

    public Promise<String> objectEncoding(K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(ENCODING).addKey(key);
        return dispatch(OBJECT, new StatusOutput<K, V>(codec), args);
    }

    public Promise<Long> objectIdletime(K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(IDLETIME).addKey(key);
        return dispatch(OBJECT, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<Long> objectRefcount(K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(REFCOUNT).addKey(key);
        return dispatch(OBJECT, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<Boolean> persist(K key) {
        return dispatch(PERSIST, new BooleanOutput<K, V>(codec), key);
    }

    public Promise<Boolean> pexpire(K key, long milliseconds) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(milliseconds);
        return dispatch(PEXPIRE, new BooleanOutput<K, V>(codec), args);
    }

    public Promise<Boolean> pexpireat(K key, Date timestamp) {
        return pexpireat(key, timestamp.getTime());
    }

    public Promise<Boolean> pexpireat(K key, long timestamp) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(timestamp);
        return dispatch(PEXPIREAT, new BooleanOutput<K, V>(codec), args);
    }

    public Promise<String> ping() {
        return dispatch(PING, new StatusOutput<K, V>(codec));
    }

    public Promise<Long> pttl(K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key);
        return dispatch(PTTL, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<Long> publish(K channel, V message) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(channel).addValue(message);
        return dispatch(PUBLISH, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<String> quit() {
        return dispatch(QUIT, new StatusOutput<K, V>(codec));
    }

    public Promise<V> randomkey() {
        return dispatch(RANDOMKEY, new ValueOutput<K, V>(codec));
    }

    public Promise<String> rename(K key, K newKey) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKey(newKey);
        return dispatch(RENAME, new StatusOutput<K, V>(codec), args);
    }

    public Promise<Boolean> renamenx(K key, K newKey) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKey(newKey);
        return dispatch(RENAMENX, new BooleanOutput<K, V>(codec), args);
    }

    public Promise<String> restore(K key, long ttl, byte[] value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(ttl).add(value);
        return dispatch(RESTORE, new StatusOutput<K, V>(codec), args);
    }

    public Promise<V> rpop(K key) {
        return dispatch(RPOP, new ValueOutput<K, V>(codec), key);
    }

    public Promise<V> rpoplpush(K source, K destination) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(source).addKey(destination);
        return dispatch(RPOPLPUSH, new ValueOutput<K, V>(codec), args);
    }

    public Promise<Long> rpush(K key, V... values) {
        return dispatch(RPUSH, new IntegerOutput<K, V>(codec), key, values);
    }

    public Promise<Long> rpushx(K key, V value) {
        return dispatch(RPUSHX, new IntegerOutput<K, V>(codec), key, value);
    }

    public Promise<Long> sadd(K key, V... members) {
        return dispatch(SADD, new IntegerOutput<K, V>(codec), key, members);
    }

    public Promise<String> save() {
        return dispatch(SAVE, new StatusOutput<K, V>(codec));
    }

    public Promise<Long> scard(K key) {
        return dispatch(SCARD, new IntegerOutput<K, V>(codec), key);
    }

    public Promise<List<Boolean>> scriptExists(String... digests) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(EXISTS);
        for (String sha : digests) args.add(sha);
        return dispatch(SCRIPT, new BooleanListOutput<K, V>(codec), args);
    }

    public Promise<String> scriptFlush() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(FLUSH);
        return dispatch(SCRIPT, new StatusOutput<K, V>(codec), args);
    }

    public Promise<String> scriptKill() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(KILL);
        return dispatch(SCRIPT, new StatusOutput<K, V>(codec), args);
    }

    public Promise<String> scriptLoad(V script) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(LOAD).addValue(script);
        return dispatch(SCRIPT, new StatusOutput<K, V>(codec), args);
    }

    public Promise<Set<V>> sdiff(K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKeys(keys);
        return dispatch(SDIFF, new ValueSetOutput<K, V>(codec), args);
    }

    public Promise<Long> sdiffstore(K destination, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(destination).addKeys(keys);
        return dispatch(SDIFFSTORE, new IntegerOutput<K, V>(codec), args);
    }

    public String select(int db) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(db);
        Command<K, V, String> cmd = dispatch(SELECT, new StatusOutput<K, V>(codec), args);
        String status = await(cmd, timeout, unit);
        if ("OK".equals(status)) this.db = db;
        return status;
    }

    public Promise<String> set(K key, V value) {
        return dispatch(SET, new StatusOutput<K, V>(codec), key, value);
    }

    public Promise<Long> setbit(K key, long offset, int value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(offset).add(value);
        return dispatch(SETBIT, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<String> setex(K key, long seconds, V value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(seconds).addValue(value);
        return dispatch(SETEX, new StatusOutput<K, V>(codec), args);
    }

    public Promise<Boolean> setnx(K key, V value) {
        return dispatch(SETNX, new BooleanOutput<K, V>(codec), key, value);
    }

    public Promise<Long> setrange(K key, long offset, V value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(offset).addValue(value);
        return dispatch(SETRANGE, new IntegerOutput<K, V>(codec), args);
    }

    @Deprecated
    public void shutdown() {
        dispatch(SHUTDOWN, new StatusOutput<K, V>(codec));
    }

    public void shutdown(boolean save) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        dispatch(SHUTDOWN, new StatusOutput<K, V>(codec), save ? args.add(SAVE) : args.add(NOSAVE));
    }

    public Promise<Set<V>> sinter(K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKeys(keys);
        return dispatch(SINTER, new ValueSetOutput<K, V>(codec), args);
    }

    public Promise<Long> sinterstore(K destination, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(destination).addKeys(keys);
        return dispatch(SINTERSTORE, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<Boolean> sismember(K key, V member) {
        return dispatch(SISMEMBER, new BooleanOutput<K, V>(codec), key, member);
    }

    public Promise<Boolean> smove(K source, K destination, V member) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(source).addKey(destination).addValue(member);
        return dispatch(SMOVE, new BooleanOutput<K, V>(codec), args);
    }

    public Promise<String> slaveof(String host, int port) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(host).add(port);
        return dispatch(SLAVEOF, new StatusOutput<K, V>(codec), args);
    }

    public Promise<String> slaveofNoOne() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(NO).add(ONE);
        return dispatch(SLAVEOF, new StatusOutput<K, V>(codec), args);
    }

    public Promise<List<Object>> slowlogGet() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(GET);
        return dispatch(SLOWLOG, new NestedMultiOutput<K, V>(codec), args);
    }

    public Promise<List<Object>> slowlogGet(int count) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(GET).add(count);
        return dispatch(SLOWLOG, new NestedMultiOutput<K, V>(codec), args);
    }

    public Promise<Long> slowlogLen() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(LEN);
        return dispatch(SLOWLOG, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<String> slowlogReset() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(RESET);
        return dispatch(SLOWLOG, new StatusOutput<K, V>(codec), args);
    }

    public Promise<Set<V>> smembers(K key) {
        return dispatch(SMEMBERS, new ValueSetOutput<K, V>(codec), key);
    }

    public Promise<List<V>> sort(K key) {
        return dispatch(SORT, new ValueListOutput<K, V>(codec), key);
    }

    public Promise<List<V>> sort(K key, SortArgs sortArgs) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key);
        sortArgs.build(args, null);
        return dispatch(SORT, new ValueListOutput<K, V>(codec), args);
    }

    public Promise<Long> sortStore(K key, SortArgs sortArgs, K destination) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key);
        sortArgs.build(args, destination);
        return dispatch(SORT, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<V> spop(K key) {
        return dispatch(SPOP, new ValueOutput<K, V>(codec), key);
    }

    public Promise<V> srandmember(K key) {
        return dispatch(SRANDMEMBER, new ValueOutput<K, V>(codec), key);
    }

    public Promise<Set<V>> srandmember(K key, long count) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(count);
        return dispatch(SRANDMEMBER, new ValueSetOutput<K, V>(codec), args);
    }

    public Promise<Long> srem(K key, V... members) {
        return dispatch(SREM, new IntegerOutput<K, V>(codec), key, members);
    }

    public Promise<Set<V>> sunion(K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKeys(keys);
        return dispatch(SUNION, new ValueSetOutput<K, V>(codec), args);
    }

    public Promise<Long> sunionstore(K destination, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(destination).addKeys(keys);
        return dispatch(SUNIONSTORE, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<String> sync() {
        return dispatch(SYNC, new StatusOutput<K, V>(codec));
    }

    public Promise<Long> strlen(K key) {
        return dispatch(STRLEN, new IntegerOutput<K, V>(codec), key);
    }

    public Promise<Long> ttl(K key) {
        return dispatch(TTL, new IntegerOutput<K, V>(codec), key);
    }

    public Promise<String> type(K key) {
        return dispatch(TYPE, new StatusOutput<K, V>(codec), key);
    }

    public Promise<String> watch(K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKeys(keys);
        return dispatch(WATCH, new StatusOutput<K, V>(codec), args);
    }

    public Promise<String> unwatch() {
        return dispatch(UNWATCH, new StatusOutput<K, V>(codec));
    }

    public Promise<Long> zadd(K key, double score, V member) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(score).addValue(member);
        return dispatch(ZADD, new IntegerOutput<K, V>(codec), args);
    }

    @SuppressWarnings("unchecked")
    public Promise<Long> zadd(K key, Object... scoresAndValues) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key);
        for (int i = 0; i < scoresAndValues.length; i += 2) {
            args.add((Double) scoresAndValues[i]);
            args.addValue((V) scoresAndValues[i + 1]);
        }
        return dispatch(ZADD, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<Long> zcard(K key) {
        return dispatch(ZCARD, new IntegerOutput<K, V>(codec), key);
    }

    public Promise<Long> zcount(K key, double min, double max) {
        return zcount(key, string(min), string(max));
    }

    public Promise<Long> zcount(K key, String min, String max) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(min).add(max);
        return dispatch(ZCOUNT, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<Double> zincrby(K key, double amount, K member) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(amount).addKey(member);
        return dispatch(ZINCRBY, new DoubleOutput<K, V>(codec), args);
    }

    public Promise<Long> zinterstore(K destination, K... keys) {
        return zinterstore(destination, new ZStoreArgs(), keys);
    }

    public Promise<Long> zinterstore(K destination, ZStoreArgs storeArgs, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(destination).add(keys.length).addKeys(keys);
        storeArgs.build(args);
        return dispatch(ZINTERSTORE, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<List<V>> zrange(K key, long start, long stop) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(start).add(stop);
        return dispatch(ZRANGE, new ValueListOutput<K, V>(codec), args);
    }

    public Promise<List<ScoredValue<V>>> zrangeWithScores(K key, long start, long stop) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(start).add(stop).add(WITHSCORES);
        return dispatch(ZRANGE, new ScoredValueListOutput<K, V>(codec), args);
    }

    public Promise<List<V>> zrangebyscore(K key, double min, double max) {
        return zrangebyscore(key, string(min), string(max));
    }

    public Promise<List<V>> zrangebyscore(K key, String min, String max) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(min).add(max);
        return dispatch(ZRANGEBYSCORE, new ValueListOutput<K, V>(codec), args);
    }

    public Promise<List<V>> zrangebyscore(K key, double min, double max, long offset, long count) {
        return zrangebyscore(key, string(min), string(max), offset, count);
    }

    public Promise<List<V>> zrangebyscore(K key, String min, String max, long offset, long count) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(min).add(max).add(LIMIT).add(offset).add(count);
        return dispatch(ZRANGEBYSCORE, new ValueListOutput<K, V>(codec), args);
    }

    public Promise<List<ScoredValue<V>>> zrangebyscoreWithScores(K key, double min, double max) {
        return zrangebyscoreWithScores(key, string(min), string(max));
    }

    public Promise<List<ScoredValue<V>>> zrangebyscoreWithScores(K key, String min, String max) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(min).add(max).add(WITHSCORES);
        return dispatch(ZRANGEBYSCORE, new ScoredValueListOutput<K, V>(codec), args);
    }

    public Promise<List<ScoredValue<V>>> zrangebyscoreWithScores(K key, double min, double max, long offset, long count) {
        return zrangebyscoreWithScores(key, string(min), string(max), offset, count);
    }

    public Promise<List<ScoredValue<V>>> zrangebyscoreWithScores(K key, String min, String max, long offset, long count) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(min).add(max).add(WITHSCORES).add(LIMIT).add(offset).add(count);
        return dispatch(ZRANGEBYSCORE, new ScoredValueListOutput<K, V>(codec), args);
    }

    public Promise<Long> zrank(K key, V member) {
        return dispatch(ZRANK, new IntegerOutput<K, V>(codec), key, member);
    }

    public Promise<Long> zrem(K key, V... members) {
        return dispatch(ZREM, new IntegerOutput<K, V>(codec), key, members);
    }

    public Promise<Long> zremrangebyrank(K key, long start, long stop) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(start).add(stop);
        return dispatch(ZREMRANGEBYRANK, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<Long> zremrangebyscore(K key, double min, double max) {
        return zremrangebyscore(key, string(min), string(max));
    }

    public Promise<Long> zremrangebyscore(K key, String min, String max) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(min).add(max);
        return dispatch(ZREMRANGEBYSCORE, new IntegerOutput<K, V>(codec), args);
    }

    public Promise<List<V>> zrevrange(K key, long start, long stop) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(start).add(stop);
        return dispatch(ZREVRANGE, new ValueListOutput<K, V>(codec), args);
    }

    public Promise<List<ScoredValue<V>>> zrevrangeWithScores(K key, long start, long stop) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(start).add(stop).add(WITHSCORES);
        return dispatch(ZREVRANGE, new ScoredValueListOutput<K, V>(codec), args);
    }

    public Promise<List<V>> zrevrangebyscore(K key, double max, double min) {
        return zrevrangebyscore(key, string(max), string(min));
    }

    public Promise<List<V>> zrevrangebyscore(K key, String max, String min) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(max).add(min);
        return dispatch(ZREVRANGEBYSCORE, new ValueListOutput<K, V>(codec), args);
    }

    public Promise<List<V>> zrevrangebyscore(K key, double max, double min, long offset, long count) {
        return zrevrangebyscore(key, string(max), string(min), offset, count);
    }

    public Promise<List<V>> zrevrangebyscore(K key, String max, String min, long offset, long count) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(max).add(min).add(LIMIT).add(offset).add(count);
        return dispatch(ZREVRANGEBYSCORE, new ValueListOutput<K, V>(codec), args);
    }

    public Promise<List<ScoredValue<V>>> zrevrangebyscoreWithScores(K key, double max, double min) {
        return zrevrangebyscoreWithScores(key, string(max), string(min));
    }

    public Promise<List<ScoredValue<V>>> zrevrangebyscoreWithScores(K key, String max, String min) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(max).add(min).add(WITHSCORES);
        return dispatch(ZREVRANGEBYSCORE, new ScoredValueListOutput<K, V>(codec), args);
    }

    public Promise<List<ScoredValue<V>>> zrevrangebyscoreWithScores(K key, double max, double min, long offset, long count) {
        return zrevrangebyscoreWithScores(key, string(max), string(min), offset, count);
    }

    public Promise<List<ScoredValue<V>>> zrevrangebyscoreWithScores(K key, String max, String min, long offset, long count) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(max).add(min).add(WITHSCORES).add(LIMIT).add(offset).add(count);
        return dispatch(ZREVRANGEBYSCORE, new ScoredValueListOutput<K, V>(codec), args);
    }

    public Promise<Long> zrevrank(K key, V member) {
        return dispatch(ZREVRANK, new IntegerOutput<K, V>(codec), key, member);
    }

    public Promise<Double> zscore(K key, V member) {
        return dispatch(ZSCORE, new DoubleOutput<K, V>(codec), key, member);
    }

    public Promise<Long> zunionstore(K destination, K... keys) {
        return zunionstore(destination, new ZStoreArgs(), keys);
    }

    public Promise<Long> zunionstore(K destination, ZStoreArgs storeArgs, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(destination).add(keys.length).addKeys(keys);
        storeArgs.build(args);
        return dispatch(ZUNIONSTORE, new IntegerOutput<K, V>(codec), args);
    }

    /**
     * Wait until commands are complete or the connection timeout is reached.
     *
     * @param Promises   Promises to wait for.
     *
     * @return True if all Promises complete in time.
     */
    public boolean awaitAll(Promise<?>... Promises) {
        return awaitAll(timeout, unit, Promises);
    }

    /**
     * Wait until Promises are complete or the supplied timeout is reached.
     *
     * @param timeout   Maximum time to wait for Promises to complete.
     * @param unit      Unit of time for the timeout.
     * @param Promises   Promises to wait for.
     *
     * @return True if all Promises complete in time.
     */
    public boolean awaitAll(long timeout, TimeUnit unit, Promise<?>... Promises) {
        boolean complete;

        try {
            long nanos = unit.toNanos(timeout);
            long time  = System.nanoTime();

            for (Promise<?> f : Promises) {
                if (nanos < 0) return false;
                f.get(nanos, TimeUnit.NANOSECONDS);
                long now = System.nanoTime();
                nanos -= now - time;
                time   = now;
            }

            complete = true;
        } catch (TimeoutException e) {
            complete = false;
        } catch (Exception e) {
            throw new RedisCommandInterruptedException(e);
        }

        return complete;
    }

    /**
     * Close the connection.
     */
    public synchronized void close() {
        if (!closed && channel != null) {
            ConnectionWatchdog watchdog = channel.pipeline().get(ConnectionWatchdog.class);
            watchdog.setReconnect(false);
            closed = true;
            channel.close();
        }
    }

    public String digest(V script) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA1");
            md.update(codec.encodeValue(script));
            return new String(Base16.encode(md.digest(), false));
        } catch (NoSuchAlgorithmException e) {
            throw new RedisException("JVM does not support SHA1");
        }
    }

    @Override
    public synchronized void channelActive(ChannelHandlerContext ctx) throws Exception {
        channel = ctx.channel();

        List<Command<K, V, ?>> tmp = new ArrayList<Command<K, V, ?>>(queue.size() + 2);

        if (password != null) {
            CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(password);
            tmp.add(new Command<K, V, String>(AUTH, new StatusOutput<K, V>(codec), args, false));
        }

        if (db != 0) {
            CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(db);
            tmp.add(new Command<K, V, String>(SELECT, new StatusOutput<K, V>(codec), args, false));
        }

        tmp.addAll(queue);
        queue.clear();

        for (Command<K, V, ?> cmd : tmp) {
            if (!cmd.isCancelled()) {
                queue.add(cmd);
                channel.writeAndFlush(cmd);
            }
        }

        tmp.clear();
    }

    @Override
    public synchronized void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (closed) {
            for (Command<K, V, ?> cmd : queue) {
                if (cmd.getOutput() != null) {
                    cmd.getOutput().setError("Connection closed");
                }
                cmd.complete();
            }
            queue.clear();
            queue = null;
            channel = null;
        }
    }

    public <T> Command<K, V, T> dispatch(CommandType type, CommandOutput<K, V, T> output) {
        return dispatch(type, output, (CommandArgs<K, V>) null);
    }

    public <T> Command<K, V, T> dispatch(CommandType type, CommandOutput<K, V, T> output, K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key);
        return dispatch(type, output, args);
    }

    public <T> Command<K, V, T> dispatch(CommandType type, CommandOutput<K, V, T> output, K key, V value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addValue(value);
        return dispatch(type, output, args);
    }

    public <T> Command<K, V, T> dispatch(CommandType type, CommandOutput<K, V, T> output, K key, V[] values) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addValues(values);
        return dispatch(type, output, args);
    }

    public synchronized <T> Command<K, V, T> dispatch(CommandType type, CommandOutput<K, V, T> output, CommandArgs<K, V> args) {
        Command<K, V, T> cmd = new Command<K, V, T>(type, output, args, multi != null);

        try {
            if (multi != null) {
                multi.add(cmd);
            }

            queue.put(cmd);

            if (channel != null) {
                channel.writeAndFlush(cmd);
            }
        } catch (NullPointerException e) {
            throw new RedisException("Connection is closed");
        } catch (InterruptedException e) {
            throw new RedisCommandInterruptedException(e);
        }

        return cmd;
    }

    public <T> T await(Command<K, V, T> cmd, long timeout, TimeUnit unit) {
        if (!cmd.await(timeout, unit)) {
            cmd.cancel(true);
            throw new RedisException("Command timed out");
        }
        CommandOutput<K, V, T> output = cmd.getOutput();
        if (output.hasError()) throw new RedisException(output.getError());
        return output.get();
    }

    @SuppressWarnings("unchecked")
    protected <K, V, T> CommandOutput<K, V, T> newScriptOutput(RedisCodec<K, V> codec, ScriptOutputType type) {
        switch (type) {
            case BOOLEAN: return (CommandOutput<K, V, T>) new BooleanOutput<K, V>(codec);
            case INTEGER: return (CommandOutput<K, V, T>) new IntegerOutput<K, V>(codec);
            case STATUS:  return (CommandOutput<K, V, T>) new StatusOutput<K, V>(codec);
            case MULTI:   return (CommandOutput<K, V, T>) new NestedMultiOutput<K, V>(codec);
            case VALUE:   return (CommandOutput<K, V, T>) new ValueOutput<K, V>(codec);
            default:      throw new RedisException("Unsupported script output type");
        }
    }

    public String string(double n) {
        if (Double.isInfinite(n)) {
            return (n > 0) ? "+inf" : "-inf";
        }
        return Double.toString(n);
    }
}
