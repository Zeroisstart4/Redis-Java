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
import static com.github.tonivade.resp.protocol.RedisToken.integer;
import static com.github.tonivade.resp.protocol.SafeString.append;

/**
 * @author zhou <br/>
 * <p>
 * redis String 类型的 append 命令实现。
 */
@Command("append")
@ParamLength(1)
@ParamType(DataType.STRING)
public class AppendCommand implements DBCommand {

    /**
     * 命令形式： append key value 追加信息到原始信息后部（如果原始信息存在就追加，否则新建）
     *
     * @param db      当前数据库
     * @param request String 类型请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {

        // value 合并
        DatabaseValue value = db.merge(safeKey(request.getParam(0)), string(request.getParam(1)),
                (oldValue, newValue) -> {
                    return string(append(oldValue.getString(), newValue.getString()));
                });

        return integer(value.getString().length());
    }

}
