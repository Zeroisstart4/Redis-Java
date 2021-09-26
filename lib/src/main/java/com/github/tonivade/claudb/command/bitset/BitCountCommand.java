/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.bitset;

import static com.github.tonivade.resp.protocol.RedisToken.integer;
import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.claudb.data.DatabaseValue.bitset;

import java.util.BitSet;

import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.command.annotation.ParamType;
import com.github.tonivade.claudb.data.DataType;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.claudb.data.DatabaseValue;

// redis Bitmaps 类型的 bitcount 命令实现。
@Command("bitcount")
@ParamLength(1)
@ParamType(DataType.STRING)
public class BitCountCommand implements DBCommand {
  // 命令形式： bitcount key 统计位集中 1 的个数
  @Override
  public RedisToken execute(Database db, Request request) {
    // 获取请求参数 key
    DatabaseValue value = db.getOrDefault(safeKey(request.getParam(0)), bitset());
    // 将该参数转为位集
    BitSet bitSet = BitSet.valueOf(value.getString().getBuffer());
    // 统计位集中 1 的个数
    return integer(bitSet.cardinality());
  }
}
