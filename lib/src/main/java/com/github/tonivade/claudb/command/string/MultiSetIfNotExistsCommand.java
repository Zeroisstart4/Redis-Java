/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.string;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.claudb.data.DatabaseKey;
import com.github.tonivade.purefun.Tuple2;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.SafeString;

import java.util.HashSet;
import java.util.Set;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.claudb.data.DatabaseValue.entry;
import static com.github.tonivade.claudb.data.DatabaseValue.string;
import static com.github.tonivade.resp.protocol.RedisToken.integer;

/**
 * @author zhou <br/>
 * <p>
 * redis String 类型的 msetnx 命令实现。
 */
@Command("msetnx")
@ParamLength(2)
public class MultiSetIfNotExistsCommand implements DBCommand {

    /**
     * 命令形式： msetnx key1 ttl1 key2 ttl2 ... 添加/修改多个数据
     *
     * @param db      当前数据库
     * @param request String 类型请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        Set<Tuple2<SafeString, SafeString>> pairs = toPairs(request);
        if (noneExists(db, pairs)) {
            pairs.forEach(entry -> db.put(safeKey(entry.get1()), string(entry.get2())));
            return integer(1);
        }
        return integer(0);
    }

    /**
     * 判断 Tuple2 是否存在，即 (key, ttl) 是否存在
     *
     * @param db    当前数据库
     * @param pairs 键值对
     * @return
     */
    private boolean noneExists(Database db, Set<Tuple2<SafeString, SafeString>> pairs) {
        return pairs.stream()
                .map(Tuple2::get1)
                .map(DatabaseKey::safeKey)
                .noneMatch(db::containsKey);
    }

    /**
     * 获取请求中的 key ttl, 并封装为 Tuple2 类型， 添加入 pairs
     *
     * @param request String 类型请求
     * @return
     */
    private Set<Tuple2<SafeString, SafeString>> toPairs(Request request) {
        Set<Tuple2<SafeString, SafeString>> pairs = new HashSet<>();
        SafeString key = null;
        for (SafeString value : request.getParams()) {
            if (key != null) {
                pairs.add(entry(key, value));
                key = null;
            } else {
                key = value;
            }
        }
        return pairs;
    }
}
