/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.key;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.claudb.data.DatabaseValue;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;

import java.time.Instant;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.resp.protocol.RedisToken.integer;

/**
 * @author zhou <br/>
 * <p>
 * TTL 命令类，模板模式，作为 TimeToLiveMillisCommand 与 TimeToLiveSecondsCommand 的父类，为其提供相应的方法
 */
public abstract class TimeToLiveCommand implements DBCommand {

    /**
     * 执行命令
     * @param db      当前数据库
     * @param request 命令请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        DatabaseValue value = db.get(safeKey(request.getParam(0)));
        if (value != null) {
            return keyExists(value);
        } else {
            return notExists();
        }
    }

    /**
     * 由子类提供具体实现，以 mills 或 seconds 为单位
     * @param value     数据库中的键
     * @param now       当前时间
     * @return
     */
    protected abstract int timeToLive(DatabaseValue value, Instant now);

    /**
     * 判断键是否存在
     * @param value 数据库中的键
     * @return
     */
    private RedisToken keyExists(DatabaseValue value) {
        if (value.getExpiredAt() != null) {
            return hasExpiredAt(value);
        } else {
            return integer(-1);
        }
    }

    /**
     * 计算剩余存活时间
     * @param value 数据库中的键
     * @return
     */
    private RedisToken hasExpiredAt(DatabaseValue value) {
        Instant now = Instant.now();
        if (!value.isExpired(now)) {
            return integer(timeToLive(value, now));
        } else {
            return notExists();
        }
    }

    /**
     * 数据库中不存在该键
     * @return
     */
    private RedisToken notExists() {
        return integer(-2);
    }
}
