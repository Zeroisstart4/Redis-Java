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
 * @author zhou <br/>
 * <p>
 * redis Hash 类型的 hvals 命令实现。
 */
@ReadOnly
@Command("hvals")
@ParamLength(1)
@ParamType(DataType.HASH)
public class HashValuesCommand implements DBCommand {

    /**
     * 命令形式： hvals key 获取哈希表中所有的字段值
     *
     * @param db      当前数据库
     * @param request 命令请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        ImmutableMap<SafeString, SafeString> map = db.getHash(request.getParam(0));
        return convert(map.values());
    }
}
