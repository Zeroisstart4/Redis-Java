/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.list;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.command.annotation.ParamType;
import com.github.tonivade.claudb.data.DataType;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.claudb.data.DatabaseValue;
import com.github.tonivade.purefun.data.ImmutableList;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.SafeString;

import java.util.ArrayList;
import java.util.List;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.claudb.data.DatabaseValue.list;
import static com.github.tonivade.resp.protocol.RedisToken.error;
import static com.github.tonivade.resp.protocol.RedisToken.status;

/**
 * @author zhou <br/>
 * <p>
 * redis List 类型的 lset 命令实现。
 */
@Command("lset")
@ParamLength(3)
@ParamType(DataType.LIST)
public class ListSetCommand implements DBCommand {

    /**
     * 命令形式： lset key index value 设置 index 位置的 list 元素的值为 value。
     * @param db      当前数据库
     * @param request 命令请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        try {
            int index = Integer.parseInt(request.getParam(1).toString());
            db.merge(safeKey(request.getParam(0)), DatabaseValue.EMPTY_LIST,
                    (oldValue, newValue) -> {
                        ImmutableList<SafeString> oldList = oldValue.getList();
                        // TODO: use Array
                        List<SafeString> array = new ArrayList<>(oldList.toList());
                        array.set(index > -1 ? index : array.size() + index, request.getParam(2));
                        return list(array);
                    });
            return status("OK");
        } catch (NumberFormatException e) {
            return error("ERR value is not an integer or out of range");
        } catch (IndexOutOfBoundsException e) {
            return error("ERR index out of range");
        }
    }
}
