/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;

import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.purefun.data.ImmutableList;
import com.github.tonivade.resp.command.ServerContext;
import com.github.tonivade.resp.protocol.RedisToken;

// 数据库服务器上下文
public interface DBServerContext extends ServerContext {

  // 默认端口
  int DEFAULT_PORT = 7081;
  // 默认 IP
  String DEFAULT_HOST = "localhost";

  // 是否为主库
  boolean isMaster();
  // 设置主库
  void setMaster(boolean master);
  // 导入 RDB
  void importRDB(InputStream input) throws IOException;
  // 导出 RDB
  void exportRDB(OutputStream output) throws IOException;
  // 获取当前数据库
  Database getDatabase(int i);
  // 获取主库
  Database getAdminDatabase();
  // 发布
  void publish(String sourceKey, RedisToken message);
  // 获取要复制的命令
  ImmutableList<RedisToken> getCommandsToReplicate();
  // 垃圾清理
  void clean(Instant now);
}
