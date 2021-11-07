/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.key;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.resp.protocol.RedisToken.error;
import static com.github.tonivade.resp.protocol.RedisToken.responseOk;

/**
 * @author zhou <br/>
 * <p>
 * redis 通用 Key 的 rename 命令实现
 */
@Command("rename")
@ParamLength(2)
public class RenameCommand implements DBCommand {

    /**
     * 命令形式： rename key newkey 将 key 重命名为 newkey
     * @param db      当前数据库
     * @param request 命令请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        if (db.rename(safeKey(request.getParam(0)), safeKey(request.getParam(1)))) {
            return responseOk();
        } else {
            return error("ERR no such key");
        }
    }
}
