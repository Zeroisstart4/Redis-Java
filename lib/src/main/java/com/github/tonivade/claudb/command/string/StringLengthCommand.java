/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */

package com.github.tonivade.claudb.command.string;

import static com.github.tonivade.resp.protocol.RedisToken.integer;
import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;

import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.SafeString;
import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.command.annotation.ParamType;
import com.github.tonivade.claudb.command.annotation.ReadOnly;
import com.github.tonivade.claudb.data.DataType;
import com.github.tonivade.claudb.data.DatabaseValue;
import com.github.tonivade.claudb.data.Database;

// redis String 类型的 strlen 命令实现。
@ReadOnly
@Command("strlen")
@ParamLength(1)
@ParamType(DataType.STRING)
public class StringLengthCommand implements DBCommand {

  /**
   *    命令形式： strlen key 获取数据字符个数（字符串长度）
   * @param db        当前数据库
   * @param request   String 类型请求
   * @return
   */
  @Override
  public RedisToken execute(Database db, Request request) {
    DatabaseValue value = db.getOrDefault(safeKey(request.getParam(0)), DatabaseValue.EMPTY_STRING);
    SafeString string = value.getString();
    return integer(string.length());
  }
}
