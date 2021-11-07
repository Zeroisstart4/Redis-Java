/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.string;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.claudb.data.DatabaseKey;
import com.github.tonivade.claudb.data.DatabaseValue;
import com.github.tonivade.purefun.Pattern1;
import com.github.tonivade.purefun.type.Option;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.SafeString;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.claudb.data.DatabaseValue.string;
import static com.github.tonivade.purefun.Matcher1.instanceOf;
import static com.github.tonivade.resp.protocol.RedisToken.*;

/**
 * @author zhou <br/>
 * <p>
 * redis String 类型的 set 命令实现。
 */
@Command("set")
@ParamLength(2)
public class SetCommand implements DBCommand {

    /**
     * 命令形式： set key value 添加/修改数据
     *
     * @param db      当前数据库
     * @param request String 类型请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        return com.github.tonivade.purefun.type.Try.of(() -> parse(request))
                .map(params -> onSuccess(db, request, params))
                .recover(this::onFailure)
                .get();
    }

    /**
     * 判断添加/修改数据是否成功
     *
     * @param db         当前数据库
     * @param request    String 类型请求
     * @param parameters 请求参数
     * @return
     */
    private RedisToken onSuccess(Database db, Request request, Parameters parameters) {
        // 获取 key
        DatabaseKey key = safeKey(request.getParam(0));
        // 解析请求参数
        DatabaseValue value = parseValue(request, parameters);
        // 判断添加参数是否存在
        return value.equals(saveValue(db, parameters, key, value)) ? responseOk() : nullString();
    }

    /**
     * 解析请求参数
     *
     * @param request    String 类型请求
     * @param parameters 请求参数
     * @return
     */
    private DatabaseValue parseValue(Request request, Parameters parameters) {
        // 获取 value
        DatabaseValue value = string(request.getParam(1));
        // 判断请求是否存在过期时间
        if (parameters.ttl != null) {
            // 更新过期时间
            value = value.expiredAt(Instant.now().plus(parameters.ttl));
        }
        return value;
    }

    /**
     * 判断添加/修改数据是否失败
     *
     * @param e 异常
     * @return
     */
    private RedisToken onFailure(Throwable e) {
        return Pattern1.<Throwable, RedisToken>build()
                .when(instanceOf(SyntaxException.class))
                .returns(error("syntax error"))
                .when(instanceOf(NumberFormatException.class))
                .returns(error("value is not an integer or out of range"))
                .otherwise()
                .returns(error("error: " + e.getMessage()))
                .apply(e);
    }

    /**
     * 数据保存
     *
     * @param db     当前数据库
     * @param params 请求参数
     * @param key    键
     * @param value  值
     * @return
     */
    private DatabaseValue saveValue(Database db, Parameters params, DatabaseKey key, DatabaseValue value) {
        DatabaseValue savedValue = null;
        if (params.ifExists) {
            savedValue = putValueIfExists(db, key, value);
        } else if (params.ifNotExists) {
            savedValue = putValueIfNotExists(db, key, value);
        } else {
            savedValue = putValue(db, key, value);
        }
        return savedValue;
    }

    /**
     * 修改数据
     *
     * @param db    当前数据库
     * @param key   键
     * @param value 值
     * @return
     */
    private DatabaseValue putValue(Database db, DatabaseKey key, DatabaseValue value) {
        db.put(key, value);
        return value;
    }

    /**
     * 如果存在数据时如何保存数据
     *
     * @param db    当前数据库
     * @param key   键
     * @param value 值
     * @return
     */
    private DatabaseValue putValueIfExists(Database db, DatabaseKey key, DatabaseValue value) {
        DatabaseValue oldValue = db.get(key);
        if (oldValue != null) {
            return putValue(db, key, value);
        }
        return oldValue;
    }

    /**
     * 如果不存在数据时如何保存数据
     *
     * @param db    当前数据库
     * @param key   键
     * @param value 值
     * @return
     */
    private DatabaseValue putValueIfNotExists(Database db, DatabaseKey key, DatabaseValue value) {
        return db.merge(key, value, (oldValue, newValue) -> oldValue);
    }

    /**
     * 请求解析
     *
     * @param request 请求
     * @return
     */
    private Parameters parse(Request request) {
        Parameters parameters = new Parameters();
        // 判断请求参数
        if (request.getLength() > 2) {
            for (int i = 2; i < request.getLength(); i++) {
                // 逐一获取参数
                SafeString option = request.getParam(i);
                // 若为 EX 则该命令为分布式锁命令
                if (match("EX", option)) {
                    // 若参数的超时时间不为空，则抛出异常
                    if (parameters.ttl != null) {
                        throw new SyntaxException();
                    }
                    // 为参数赋予超时时间
                    parameters.ttl = parseTtl(request, ++i)
                            .map(Duration::ofSeconds)
                            .getOrElseThrow(SyntaxException::new);
                } else if (match("PX", option)) {
                    if (parameters.ttl != null) {
                        throw new SyntaxException();
                    }
                    parameters.ttl = parseTtl(request, ++i)
                            .map(Duration::ofMillis)
                            .getOrElseThrow(SyntaxException::new);
                } else if (match("NX", option)) {
                    if (parameters.ifExists) {
                        throw new SyntaxException();
                    }
                    parameters.ifNotExists = true;
                } else if (match("XX", option)) {
                    if (parameters.ifNotExists) {
                        throw new SyntaxException();
                    }
                    parameters.ifExists = true;
                } else {
                    throw new SyntaxException();
                }
            }
        }
        return parameters;
    }


    /**
     * 判断是否相同
     *
     * @param string 源串
     * @param option 模式串
     * @return
     */
    private boolean match(String string, SafeString option) {
        return string.equalsIgnoreCase(option.toString());
    }

    /**
     * 解析 TTL
     *
     * @param request String 类型请求
     * @param i       TTL 参数对应的索引位
     * @return
     */
    private Option<Integer> parseTtl(Request request, int i) {
        Option<SafeString> ttlOption = request.getOptionalParam(i);
        return ttlOption.map(SafeString::toString).map(Integer::parseInt);
    }

    /**
     * 参数类，用于整合 String 的信息
     */
    private static class Parameters {
        // 是否存在
        private boolean ifExists;
        private boolean ifNotExists;
        // 是否存在超时时间
        private TemporalAmount ttl;
    }

    /**
     * 语法异常类
     */
    private static class SyntaxException extends RuntimeException {
        private static final long serialVersionUID = 6960370945568192189L;
    }
}
