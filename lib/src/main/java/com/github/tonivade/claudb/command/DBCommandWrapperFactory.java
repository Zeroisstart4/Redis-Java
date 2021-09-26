/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command;

import com.github.tonivade.resp.command.CommandWrapperFactory;
import com.github.tonivade.resp.command.RespCommand;

// 数据库命令包装器工厂类
public class DBCommandWrapperFactory implements CommandWrapperFactory {
  @Override
  public RespCommand wrap(Object command) {
    return new DBCommandWrapper(command);
  }
}
