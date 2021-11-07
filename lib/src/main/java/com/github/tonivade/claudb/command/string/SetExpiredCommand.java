/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.string;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.SafeString;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.claudb.data.DatabaseValue.string;
import static com.github.tonivade.resp.protocol.RedisToken.error;
import static com.github.tonivade.resp.protocol.RedisToken.responseOk;

/**
 * @author zhou <br/>
 * <p>
 * redis String 类型的 setex 命令实现。
 */
@Command("setex")
@ParamLength(3)
public class SetExpiredCommand implements DBCommand {

    /**
     * 命令形式： setex key seconds value 设置数据具有指定的生命周期
     *
     * @param db      当前数据库
     * @param request String 类型请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        try {
            db.put(safeKey(request.getParam(0)), string(request.getParam(2))
                    .expiredAt(parseTtl(request.getParam(1))));
            return responseOk();
        } catch (NumberFormatException e) {
            return error("ERR value is not an integer or out of range");
        }
    }

    /**
     * 解析超时时间
     *
     * @param safeString
     * @return
     */
    private int parseTtl(SafeString safeString) {
        return Integer.parseInt(safeString.toString());
    }
}
