/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command;

import java.util.Collection;

import com.github.tonivade.claudb.DBServerContext;
import com.github.tonivade.claudb.DBServerState;
import com.github.tonivade.claudb.DBSessionState;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.claudb.data.DatabaseValue;
import com.github.tonivade.purefun.data.Sequence;
import com.github.tonivade.purefun.type.Option;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.command.ServerContext;
import com.github.tonivade.resp.command.Session;
import com.github.tonivade.resp.protocol.RedisToken;

// 数据库命令
@FunctionalInterface
public interface DBCommand {

  /**
   * 执行请求获取 RedisToken
   * @param db        当前数据库
   * @param request   命令请求
   * @return
   */
  RedisToken execute(Database db, Request request);

  /**
   * 数据库服务器上下文
   * @param server  服务器上下文
   * @return
   */
  default DBServerContext getClauDB(ServerContext server) {
    return (DBServerContext) server;
  }

  /**
   * 获取主库
   * @param server  服务器上下文
   * @return
   */
  default Database getAdminDatabase(ServerContext server) {
    return getServerState(server).getAdminDatabase();
  }

  /**
   * 获取从库
   * @param server
   * @return
   */
  default DBServerState getServerState(ServerContext server) {
    return serverState(server).getOrElseThrow(() -> new IllegalStateException("missing server state"));
  }

  /**
   * 获取会话状态
   * @param session
   * @return
   */
  default DBSessionState getSessionState(Session session) {
    return sessionState(session).getOrElseThrow(() -> new IllegalStateException("missiong session state"));
  }

  /**
   * 获取从库状态
   * @param server
   * @return
   */
  default Option<DBServerState> serverState(ServerContext server) {
    return server.getValue("state");
  }

  /**
   * 获取会话状态
   * @param session
   * @return
   */
  default Option<DBSessionState> sessionState(Session session) {
    return session.getValue("state");
  }

  /**
   * 类型转换 DatabaseValue 转 RedisToken
   * @param value
   * @return
   */
  default RedisToken convert(DatabaseValue value) {
    return DBResponse.convertValue(value);
  }

  default RedisToken convert(Collection<?> list) {
    return DBResponse.convertArray(list);
  }

  default RedisToken convert(Sequence<?> list) {
    return DBResponse.convertArray(list.asList().toList());
  }
}
