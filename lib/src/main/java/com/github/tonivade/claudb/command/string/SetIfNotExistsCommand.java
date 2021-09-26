/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.string;

import static com.github.tonivade.resp.protocol.RedisToken.integer;
import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.claudb.data.DatabaseValue.string;

import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.claudb.data.DatabaseKey;
import com.github.tonivade.claudb.data.DatabaseValue;

// redis String 类型的 setnx 命令实现。
@Command("setnx")
@ParamLength(2)
public class SetIfNotExistsCommand implements DBCommand {

  /**
   *      命令形式： setnx key value 设置分布式锁
   * @param db        当前数据库
   * @param request   String 类型请求
   * @return
   */
  @Override
  public RedisToken execute(Database db, Request request) {
    DatabaseKey key = safeKey(request.getParam(0));
    DatabaseValue value = string(request.getParam(1));
    return integer(putValueIfNotExists(db, key, value).equals(value));
  }

  /**
   *    当值不存在时，添加该值
   * @param db      当前数据库
   * @param key     键
   * @param value   值
   * @return
   */
  private DatabaseValue putValueIfNotExists(Database db, DatabaseKey key, DatabaseValue value) {
    return db.merge(key, value, (oldValue, newValue) -> oldValue);
  }
}
