/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.server;

import com.github.tonivade.claudb.DBServerState;
import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.command.annotation.ReadOnly;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.claudb.data.DatabaseValue;
import com.github.tonivade.purefun.data.ImmutableList;
import com.github.tonivade.purefun.data.ImmutableMap;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.SafeString;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.resp.protocol.RedisToken.*;
import static com.github.tonivade.resp.protocol.SafeString.safeString;
import static java.lang.Integer.parseInt;

/**
 * @author zhou <br/>
 * <p>
 * redis 数据库相关命令的 role 命令实现。
 */
@ReadOnly
@Command("role")
public class RoleCommand implements DBCommand {

    /**
     * 命令形式： role 返回实例当前是 master，slave 还是 sentinel
     * @param db      当前数据库
     * @param request 命令请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        DBServerState serverState = getServerState(request.getServerContext());
        Database adminDatabase = getAdminDatabase(request.getServerContext());
        return serverState.isMaster() ? master(adminDatabase) : slave(adminDatabase);
    }

    /**
     * 从服务器
     * @param adminDatabase
     * @return
     */
    private RedisToken slave(Database adminDatabase) {
        ImmutableMap<SafeString, SafeString> hash = adminDatabase.getHash(safeString("master"));
        return array(string("slave"),
                string(hash.get(safeString("host")).get()),
                integer(hash.get(safeString("port")).map(port -> parseInt(port.toString())).get()),
                string(hash.get(safeString("state")).get()), integer(0));
    }

    /**
     * 主服务器
     * @param adminDatabase
     * @return
     */
    private RedisToken master(Database adminDatabase) {
        return array(string("master"), integer(0), array(slaves(adminDatabase)));
    }

    /**
     * 获取主服务器的所有从服务器
     * @param adminDatabase
     * @return
     */
    private ImmutableList<RedisToken> slaves(Database adminDatabase) {
        DatabaseValue value = adminDatabase.getOrDefault(safeKey("slaves"), DatabaseValue.EMPTY_SET);
        ImmutableList<SafeString> set = value.getSet().asList().sort(SafeString::compareTo);
        return set.map(SafeString::toString)
                .map(slave -> slave.split(":"))
                .map(slave -> array(string(slave[0]), string(slave[1]), string("0"))).asList();
    }
}
