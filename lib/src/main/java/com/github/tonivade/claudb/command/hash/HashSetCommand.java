/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.hash;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.claudb.data.DatabaseValue.entry;
import static com.github.tonivade.claudb.data.DatabaseValue.hash;
import static com.github.tonivade.resp.protocol.RedisToken.integer;

import java.util.HashMap;
import java.util.Map;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.command.annotation.ParamType;
import com.github.tonivade.claudb.data.DataType;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.claudb.data.DatabaseValue;
import com.github.tonivade.purefun.data.ImmutableMap;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.SafeString;

// redis Hash 类型的 hset 命令实现。
@Command("hset")
@ParamLength(3)
@ParamType(DataType.HASH)
public class HashSetCommand implements DBCommand {

    /**
     * 命令形式： hset key field value 添加/修改数据
     * @param db        当前数据库
     * @param request   命令请求
     * @return
     */
  @Override
  public RedisToken execute(Database db, Request request) {
    DatabaseValue value = hash(entry(request.getParam(1), request.getParam(2)));

    DatabaseValue resultValue = db.merge(safeKey(request.getParam(0)), value,
        (oldValue, newValue) -> {
          Map<SafeString, SafeString> merge = new HashMap<>();
          merge.putAll(oldValue.getHash().toMap());
          merge.putAll(newValue.getHash().toMap());
          return hash(ImmutableMap.from(merge));
        });

    ImmutableMap<SafeString, SafeString> resultMap = resultValue.getHash();

    return integer(resultMap.get(request.getParam(1)) == null);
  }
}
