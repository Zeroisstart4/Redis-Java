/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.key;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.claudb.data.DatabaseValue;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.SafeString;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.resp.protocol.RedisToken.error;
import static com.github.tonivade.resp.protocol.RedisToken.integer;

/**
 * @author zhou <br/>
 * <p>
 * redis 通用 Key 的 expire 命令实现
 */
@Command("expire")
@ParamLength(2)
public class ExpireCommand implements DBCommand {

    /**
     *命令形式： expire key seconds 设置 key 的过期时间，超过时间后，将会自动删除该 key
     * @param db      当前数据库
     * @param request 命令请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        try {
            DatabaseValue value = db.get(safeKey(request.getParam(0)));
            if (value != null) {
                db.put(safeKey(request.getParam(0)), value.expiredAt(parseTtl(request.getParam(1))));
            }
            return integer(value != null);
        } catch (NumberFormatException e) {
            return error("ERR value is not an integer or out of range");
        }
    }

    /**
     * 解析 TTL 超时时间
     * @param param
     * @return
     */
    private int parseTtl(SafeString param) {
        return Integer.parseInt(param.toString());
    }
}
