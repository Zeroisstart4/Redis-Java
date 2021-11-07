/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */

package com.github.tonivade.claudb.command.server;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.command.annotation.ReadOnly;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;

import static com.github.tonivade.resp.protocol.RedisToken.error;
import static com.github.tonivade.resp.protocol.RedisToken.responseOk;
import static java.lang.Integer.parseInt;

/**
 * @author zhou <br/>
 * <p>
 * redis 数据库相关命令的 select 命令实现。
 */
@ReadOnly
@Command("select")
@ParamLength(1)
public class SelectCommand implements DBCommand {

    /**
     * 命令形式： select index 选择一个数据库，下标值从0开始，一个新连接默认连接的数据库是DB0。
     * @param db      当前数据库
     * @param request 命令请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        try {
            getSessionState(request.getSession()).setCurrentDB(parseCurrentDB(request));
            return responseOk();
        } catch (NumberFormatException e) {
            return error("ERR invalid DB index");
        }
    }

    /**
     * 返回当前数据库对应的索引
     * @param request
     * @return
     */
    private int parseCurrentDB(Request request) {
        return parseInt(request.getParam(0).toString());
    }
}
