/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.hash;

import java.util.ArrayList;
import java.util.List;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.command.annotation.ParamType;
import com.github.tonivade.claudb.command.annotation.ReadOnly;
import com.github.tonivade.claudb.data.DataType;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.purefun.data.ImmutableMap;
import com.github.tonivade.purefun.type.Option;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.SafeString;

// redis Hash 类型的 hmget 命令实现。
@ReadOnly
@Command("hmget")
@ParamLength(2)
@ParamType(DataType.HASH)
public class HashMultiGetCommand implements DBCommand {

  /**
   * 命令形式： hmget key field1 field2 …  获取多个数据
   * @param db        当前数据库
   * @param request   命令请求
   * @return
   */
  @Override
  public RedisToken execute(Database db, Request request) {

    ImmutableMap<SafeString, SafeString> map = db.getHash(request.getParam(0));

    List<RedisToken> rtList = new ArrayList<>();

    for (int paramNumber = 1; paramNumber < request.getParams().size(); paramNumber++) {
      Option<SafeString> oss = map.get(request.getParam(paramNumber));
      rtList.add(oss.map(RedisToken::string).getOrElse(RedisToken::nullString));
    }

    return convert(rtList);
  }

}
