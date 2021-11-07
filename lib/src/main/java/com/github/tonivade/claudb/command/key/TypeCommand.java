/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.key;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.command.annotation.ReadOnly;
import com.github.tonivade.claudb.data.DataType;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.claudb.data.DatabaseValue;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.resp.protocol.RedisToken.status;

/**
 * @author zhou <br/>
 *
 * redis 通用 Key 的 type 命令实现
 */
@ReadOnly
@Command("type")
@ParamLength(1)
public class TypeCommand implements DBCommand {

    /**
     * 命令形式： type key 返回 key 所存储的 value 的数据结构类型
     * @param db      当前数据库
     * @param request 命令请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        DatabaseValue value = db.get(safeKey(request.getParam(0)));
        if (value != null) {
            return status(value.getType().text());
        } else {
            return status(DataType.NONE.text());
        }
    }
}
