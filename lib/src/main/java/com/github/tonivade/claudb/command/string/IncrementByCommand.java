/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.string;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.command.annotation.ParamType;
import com.github.tonivade.claudb.data.DataType;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.claudb.data.DatabaseValue;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.claudb.data.DatabaseValue.string;
import static com.github.tonivade.resp.protocol.RedisToken.error;
import static com.github.tonivade.resp.protocol.RedisToken.integer;

/**
 * @author zhou <br/>
 * <p>
 * redis String 类型的 incrby 命令实现。
 */
@Command("incrby")
@ParamLength(2)
@ParamType(DataType.STRING)
public class IncrementByCommand implements DBCommand {

    /**
     * 命令形式： incrby key increment 设置数值数据增加指定范围的值
     *
     * @param db      当前数据库
     * @param request String 类型请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        try {
            DatabaseValue value = db.merge(safeKey(request.getParam(0)), string(request.getParam(1)),
                    (oldValue, newValue) -> {
                        int increment = Integer.parseInt(newValue.getString().toString());
                        int current = Integer.parseInt(oldValue.getString().toString());
                        return string(String.valueOf(current + increment));
                    });
            return integer(Integer.parseInt(value.getString().toString()));
        } catch (NumberFormatException e) {
            return error("ERR value is not an integer or out of range");
        }
    }
}
