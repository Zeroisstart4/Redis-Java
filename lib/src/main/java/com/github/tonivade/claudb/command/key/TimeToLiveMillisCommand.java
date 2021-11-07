/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.key;

import com.github.tonivade.claudb.data.DatabaseValue;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;

import java.time.Instant;

/**
 * @author zhou <br/>
 * <p>
 * redis 通用 Key 的 pttl 命令实现
 */
@Command("pttl")
@ParamLength(1)
public class TimeToLiveMillisCommand extends TimeToLiveCommand {

    /**
     * 命令形式： pttl key 以毫秒为单位返回 key 的剩余生存时间
     * @param value     数据库中的键
     * @param now       当前时间
     * @return
     */
    @Override
    protected int timeToLive(DatabaseValue value, Instant now) {
        return (int) value.timeToLiveMillis(now);
    }
}
