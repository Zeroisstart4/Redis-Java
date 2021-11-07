/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.string;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.command.annotation.ParamType;
import com.github.tonivade.claudb.data.DataType;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.claudb.data.DatabaseValue.string;

/**
 * @author zhou <br/>
 * <p>
 * redis String 类型的 getset 命令实现。
 */
@Command("getset")
@ParamLength(2)
@ParamType(DataType.STRING)
public class GetSetCommand implements DBCommand {

    /**
     * 命令形式： getset key value 原子的设置 key 值，并返回旧的 key 值
     *
     * @param db      当前数据库
     * @param request String 类型请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        return convert(db.put(safeKey(request.getParam(0)), string(request.getParam(1))));
    }
}
