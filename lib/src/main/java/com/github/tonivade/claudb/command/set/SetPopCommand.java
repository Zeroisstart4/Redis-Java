/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.set;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.command.annotation.ParamType;
import com.github.tonivade.claudb.data.DataType;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.claudb.data.DatabaseValue;
import com.github.tonivade.purefun.data.ImmutableArray;
import com.github.tonivade.purefun.data.ImmutableSet;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.SafeString;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.claudb.data.DatabaseValue.set;
import static com.github.tonivade.resp.protocol.RedisToken.nullString;
import static com.github.tonivade.resp.protocol.RedisToken.string;

/**
 * @author zhou <br/>
 * <p>
 * redis Set 类型的 spop 命令实现。
 */
@Command("spop")
@ParamLength(1)
@ParamType(DataType.SET)
public class SetPopCommand implements DBCommand {

    /**
     * 命令形式： spop key [count] 从存储在 key 的集合中移除并返回一个或多个随机元素。
     * @param db      当前数据库
     * @param request 命令请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        List<SafeString> removed = new LinkedList<>();
        db.merge(safeKey(request.getParam(0)), DatabaseValue.EMPTY_SET,
                (oldValue, newValue) -> {
                    ImmutableSet<SafeString> oldSet = oldValue.getSet();
                    SafeString item = getRandomItem(oldSet.asArray());
                    removed.add(item);
                    return set(oldSet.remove(item));
                });
        if (removed.isEmpty()) {
            return nullString();
        } else {
            return string(removed.get(0));
        }
    }

    private SafeString getRandomItem(ImmutableArray<SafeString> array) {
        return array.get(random(array));
    }

    private int random(ImmutableArray<?> array) {
        return new Random().nextInt(array.size());
    }
}
