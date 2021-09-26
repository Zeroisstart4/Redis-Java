/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.hash;

import static com.github.tonivade.resp.protocol.RedisToken.array;
import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;

import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.claudb.command.DBCommand;

import com.github.tonivade.claudb.command.annotation.ParamType;
import com.github.tonivade.claudb.command.annotation.ReadOnly;
import com.github.tonivade.claudb.data.DataType;
import com.github.tonivade.claudb.data.DatabaseValue;
import com.github.tonivade.claudb.data.Database;

// redis Hash 类型的 hgetall 命令实现。
@ReadOnly
@Command("hgetall")
@ParamLength(1)
@ParamType(DataType.HASH)
public class HashGetAllCommand implements DBCommand {

  /**
   * 命令形式： hgetall key  获取所有数据
   * @param db        当前数据库
   * @param request   命令请求
   * @return
   */
  @Override
  public RedisToken execute(Database db, Request request) {
    DatabaseValue value = db.get(safeKey(request.getParam(0)));
    if (value != null) {
      return convert(value);
    } else {
      return array();
    }
  }
}
