/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.string;

import static com.github.tonivade.resp.protocol.RedisToken.status;
import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.claudb.data.DatabaseValue.string;

import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.SafeString;
import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.data.Database;

// redis String 类型的 mset 命令实现。
@Command("mset")
@ParamLength(2)
public class MultiSetCommand implements DBCommand {

  /**
   *      命令形式： mset key1 key2 ... 添加/修改多个数据
   * @param db        当前数据库
   * @param request   String 类型请求
   * @return
   */
  @Override
  public RedisToken execute(Database db, Request request) {
    SafeString key = null;
    for (SafeString value : request.getParams()) {
      if (key != null) {
        db.put(safeKey(key), string(value));
        key = null;
      } else {
        key = value;
      }
    }
    return status("OK");
  }
}
