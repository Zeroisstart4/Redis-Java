/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.zset;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.command.annotation.ParamType;
import com.github.tonivade.claudb.command.annotation.ReadOnly;
import com.github.tonivade.claudb.data.DataType;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.claudb.data.DatabaseValue;
import com.github.tonivade.purefun.type.Option;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.SafeString;

import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.stream.Stream;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.resp.protocol.RedisToken.error;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

/**
 * @author zhou <br/>
 * <p>
 * redis zset 类型的 zrange 命令实现。
 */
@ReadOnly
@Command("zrange")
@ParamLength(3)
@ParamType(DataType.ZSET)
public class SortedSetRangeCommand implements DBCommand {

    private static final String PARAM_WITHSCORES = "WITHSCORES";

    /**
     * 命令形式： zrange key start stop [WITHSCORES] 返回存储在有序集合key中的指定范围的元素。
     * 返回的元素可以认为是按得分从最低到最高排列。 如果得分相同，将按字典排序。
     * @param db      当前数据库
     * @param request 命令请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        try {
            DatabaseValue value = db.getOrDefault(safeKey(request.getParam(0)), DatabaseValue.EMPTY_ZSET);
            NavigableSet<Entry<Double, SafeString>> set = value.getSortedSet();

            int from = Integer.parseInt(request.getParam(1).toString());
            if (from < 0) {
                from = set.size() + from;
            }
            int to = Integer.parseInt(request.getParam(2).toString());
            if (to < 0) {
                to = set.size() + to;
            }

            List<Object> result = emptyList();
            if (from <= to) {
                Option<SafeString> withScores = request.getOptionalParam(3);
                if (withScores.isPresent() && withScores.get().toString().equalsIgnoreCase(PARAM_WITHSCORES)) {
                    result = set.stream().skip(from).limit((to - from) + 1L)
                            .flatMap(entry -> Stream.of(entry.getValue(), entry.getKey())).collect(toList());
                } else {
                    result = set.stream().skip(from).limit((to - from) + 1L)
                            .map(Entry::getValue).collect(toList());
                }
            }

            return convert(result);
        } catch (NumberFormatException e) {
            return error("ERR value is not an integer or out of range");
        }
    }
}
