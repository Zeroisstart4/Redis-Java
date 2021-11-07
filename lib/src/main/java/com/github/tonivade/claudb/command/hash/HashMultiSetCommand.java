/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.hash;

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

import java.util.HashMap;
import java.util.Map;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.claudb.data.DatabaseValue.entry;
import static com.github.tonivade.claudb.data.DatabaseValue.hash;
import static com.github.tonivade.resp.protocol.RedisToken.responseOk;

/**
 * @author zhou <br/>
 * <p>
 * redis Hash 类型的 hmset 命令实现。
 */
@Command("hmset")
@ParamLength(3)
@ParamType(DataType.HASH)
public class HashMultiSetCommand implements DBCommand {

    /**
     * 命令形式： hmset key field1 value1 field2 value2 …  添加/修改多个数据
     *
     * @param db      当前数据库
     * @param request 命令请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {

        for (int paramNumber = 1; paramNumber < request.getParams().size(); paramNumber += 2) {

            SafeString mapKey = request.getParam(paramNumber);
            SafeString mapVal = request.getParam(paramNumber + 1);

            DatabaseValue value = hash(entry(mapKey, mapVal));

            db.merge(safeKey(request.getParam(0)), value,
                    (oldValue, newValue) -> {
                        Map<SafeString, SafeString> merge = new HashMap<>();
                        merge.putAll(oldValue.getHash().toMap());
                        merge.putAll(newValue.getHash().toMap());
                        return hash(ImmutableMap.from(merge));
                    }
            );

        }

        return responseOk();
    }
}
