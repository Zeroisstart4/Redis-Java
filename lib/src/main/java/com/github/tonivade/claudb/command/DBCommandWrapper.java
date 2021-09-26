/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.resp.protocol.RedisToken.error;
import static com.github.tonivade.resp.protocol.RedisToken.status;

import com.github.tonivade.claudb.DBServerState;
import com.github.tonivade.claudb.DBSessionState;
import com.github.tonivade.claudb.TransactionState;
import com.github.tonivade.claudb.command.annotation.ParamType;
import com.github.tonivade.claudb.command.annotation.PubSubAllowed;
import com.github.tonivade.claudb.command.annotation.ReadOnly;
import com.github.tonivade.claudb.command.annotation.TxIgnore;
import com.github.tonivade.claudb.data.DataType;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.purefun.type.Option;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.command.RespCommand;
import com.github.tonivade.resp.command.ServerContext;
import com.github.tonivade.resp.command.Session;
import com.github.tonivade.resp.protocol.RedisToken;

// 数据库命令包装器
public class DBCommandWrapper implements RespCommand {

  // 请求参数
  private int params;
  // 数据类型
  private DataType dataType;
  // 是否允许发布订阅
  private final boolean pubSubAllowed;
  // 是否忽略事务
  private final boolean txIgnore;
  // 是否为只读状态
  private final boolean readOnly;
  // 命令
  private final Object command;

  public DBCommandWrapper(Object command) {
    this.command = command;
    ParamLength length = command.getClass().getAnnotation(ParamLength.class);
    if (length != null) {
      this.params = length.value();
    }
    ParamType type = command.getClass().getAnnotation(ParamType.class);
    if (type != null) {
      this.dataType = type.value();
    }
    this.readOnly = command.getClass().isAnnotationPresent(ReadOnly.class);
    this.txIgnore = command.getClass().isAnnotationPresent(TxIgnore.class);
    this.pubSubAllowed = command.getClass().isAnnotationPresent(PubSubAllowed.class);
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  public boolean isTxIgnore() {
    return txIgnore;
  }

  public boolean isPubSubAllowed() {
    return pubSubAllowed;
  }

  /**
   *  执行命令
   * @param request
   * @return
   */
  @Override
  public RedisToken execute(Request request) {
    // FIXME: ugly piece of code, please refactor
    // 获取当前数据库
    Database db = getCurrentDB(request);
    // 若请求长度小于参数长度，报错
    if (request.getLength() < params) {
      return error("ERR wrong number of arguments for '" + request.getCommand() + "' command");
    }
    // 类型不符，报错
    else if (dataType != null && !db.isType(safeKey(request.getParam(0)), dataType)) {
      return error("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    // 发布订阅状态不符，报错
    else if (isSubscribed(request) && !pubSubAllowed) {
      return error("ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / QUIT allowed in this context");
    }
    // 事务支持状态，报错
    else if (isTxActive(request) && !txIgnore) {
      enqueueRequest(request);
      return status("QUEUED");
    }
    if (command instanceof DBCommand) {
      return executeDBCommand(db, request);
    } else if (command instanceof RespCommand) {
      return executeCommand(request);
    }
    return error("invalid command type: " + command.getClass());
  }

  private RedisToken executeCommand(Request request) {
    return ((RespCommand) command).execute(request);
  }

  private RedisToken executeDBCommand(Database db, Request request) {
    return ((DBCommand) command).execute(db, request);
  }

  private void enqueueRequest(Request request) {
    getTransactionState(request.getSession()).ifPresent(tx -> tx.enqueue(request));
  }

  private boolean isTxActive(Request request) {
    return getTransactionState(request.getSession()).isPresent();
  }

  private Option<TransactionState> getTransactionState(Session session) {
    return session.getValue("tx");
  }

  private Database getCurrentDB(Request request) {
    DBServerState serverState = getServerState(request.getServerContext());
    DBSessionState sessionState = getSessionState(request.getSession());
    return serverState.getDatabase(sessionState.getCurrentDB());
  }

  private DBServerState getServerState(ServerContext server) {
    return serverState(server).getOrElseThrow(() -> new IllegalStateException("missing server state"));
  }

  private DBSessionState getSessionState(Session session) {
    return sessionState(session).getOrElseThrow(() -> new IllegalStateException("missing session state"));
  }

  private Option<DBServerState> serverState(ServerContext server) {
    return server.getValue("state");
  }

  private Option<DBSessionState> sessionState(Session session) {
    return session.getValue("state");
  }

  private boolean isSubscribed(Request request) {
    return getSessionState(request.getSession()).isSubscribed();
  }
}