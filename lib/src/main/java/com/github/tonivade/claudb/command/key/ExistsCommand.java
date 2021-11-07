/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.key;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.command.annotation.ReadOnly;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.claudb.data.DatabaseValue;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;

import java.time.Instant;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.resp.protocol.RedisToken.integer;

/**
 * @author zhou <br/>
 * <p>
 * redis 通用 Key 的 exists 命令实现。
 */
@ReadOnly
@Command("exists")
@ParamLength(1)
public class ExistsCommand implements DBCommand {

    /**
     * 命令形式： exists key 返回 key 是否存在。
     * @param db      当前数据库
     * @param request 命令请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        DatabaseValue value = db.get(safeKey(request.getParam(0)));
        return integer(value != null ? !value.isExpired(Instant.now()) : false);
    }
}
