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

import static com.github.tonivade.resp.protocol.RedisToken.integer;

/**
 * @author zhou <br/>
 * redis Hash 类型的 hexists 命令实现。
 */
@ReadOnly
@Command("hexists")
@ParamLength(2)
@ParamType(DataType.HASH)
public class HashExistsCommand implements DBCommand {

    /**
     * 命令形式： hexists key field 获取哈希表中是否存在指定的字段
     *
     * @param db      当前数据库
     * @param request 命令请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {

        // 获取 field 对应的 hash 表
        ImmutableMap<SafeString, SafeString> map = db.getHash(request.getParam(0));
        // 判断是否存在该 hash 表
        return integer(map.containsKey(request.getParam(1)));
    }
}
