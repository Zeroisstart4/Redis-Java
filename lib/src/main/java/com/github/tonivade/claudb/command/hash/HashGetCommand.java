/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.hash;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.command.annotation.ParamType;
import com.github.tonivade.claudb.command.annotation.ReadOnly;
import com.github.tonivade.claudb.data.DataType;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.purefun.data.ImmutableMap;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.SafeString;

/**
 * @author <br/>
 * <p>
 * redis Hash 类型的 hget 命令实现。
 */
@ReadOnly
@Command("hget")
@ParamLength(2)
@ParamType(DataType.HASH)
public class HashGetCommand implements DBCommand {

    /**
     * 命令形式： hget key field 获取数据
     *
     * @param db      当前数据库
     * @param request 命令请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {

        // 获取 hash 表
        ImmutableMap<SafeString, SafeString> map = db.getHash(request.getParam(0));
        // 流式编程，返回满足条件的元素集合
        return map.get(request.getParam(1))
                .map(RedisToken::string)
                .getOrElse(RedisToken::nullString);
    }
}
