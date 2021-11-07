/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */

package com.github.tonivade.claudb.command.server;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;

import static com.github.tonivade.resp.protocol.RedisToken.responseOk;

/**
 * @author zhou <br/>
 * <p>
 * redis 数据库相关命令的 flushdb 命令实现。
 */
@Command("flushdb")
public class FlushDBCommand implements DBCommand {

    /**
     * 命令形式： flushdb 删除当前数据库里面的所有数据。
     * @param db      当前数据库
     * @param request 命令请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        db.clear();
        return responseOk();
    }
}
