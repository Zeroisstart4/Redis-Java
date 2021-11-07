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
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.SafeString;

import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.stream.Stream;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.claudb.data.DatabaseValue.score;
import static com.github.tonivade.resp.protocol.RedisToken.error;
import static java.lang.Integer.parseInt;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

/**
 * @author zhou <br/>
 * <p>
 * redis zset 类型的 zrangebyscore 命令实现。
 */
@ReadOnly
@Command("zrangebyscore")
@ParamLength(3)
@ParamType(DataType.ZSET)
public class SortedSetRangeByScoreCommand implements DBCommand {


    private static final String EXCLUSIVE = "(";
    /**
     * 下限值
     */
    private static final String MINUS_INFINITY = "-inf";
    /**
     * 上限值
     */
    private static final String INIFITY = "+inf";
    /**
     * 添加分数值
     */
    private static final String PARAM_WITHSCORES = "WITHSCORES";
    /**
     * 限制条件
     */
    private static final String PARAM_LIMIT = "LIMIT";


    /**
     * 命令形式： zrangebyscore key min max [WITHSCORES] [LIMIT offset count]
     *
     * 如果M是常量（比如，用limit总是请求前10个元素），你可以认为是O(log(N))。
     * 返回key的有序集合中的分数在min和max之间的所有元素（包括分数等于max或者min的元素）。元素被认为是从低分到高分排序的。
     * 具有相同分数的元素按字典序排列（这个根据redis对有序集合实现的情况而定，并不需要进一步计算）。
     * 可选的LIMIT参数指定返回结果的数量及区间（类似SQL中SELECT LIMIT offset, count）。
     * 注意，如果offset太大，定位offset就可能遍历整个有序集合，这会增加O(N)的复杂度。
     * 可选参数WITHSCORES会返回元素和其分数，而不只是元素。这个选项在redis2.0之后的版本都可用。
     * ##区间及无限
     * min和max可以是-inf和+inf，这样一来，你就可以在不知道有序集的最低和最高score值的情况下，使用ZRANGEBYSCORE这类命令。
     * 默认情况下，区间的取值使用闭区间(小于等于或大于等于)，你也可以通过给参数前增加(符号来使用可选的开区间(小于或大于)。
     *
     * @param db      当前数据库
     * @param request 命令请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        try {
            DatabaseValue value = db.getOrDefault(safeKey(request.getParam(0)), DatabaseValue.EMPTY_ZSET);
            NavigableSet<Entry<Double, SafeString>> set = value.getSortedSet();

            float from = parseRange(request.getParam(1).toString());
            float to = parseRange(request.getParam(2).toString());

            Options options = parseOptions(request);

            Set<Entry<Double, SafeString>> range = set.subSet(
                    score(from, SafeString.EMPTY_STRING), inclusive(request.getParam(1)),
                    score(to, SafeString.EMPTY_STRING), inclusive(request.getParam(2)));

            List<Object> result = emptyList();
            if (from <= to) {
                if (options.withScores) {
                    result = range.stream().flatMap(
                            entry -> Stream.of(entry.getValue(), entry.getKey())).collect(toList());
                } else {
                    result = range.stream().map(Entry::getValue).collect(toList());
                }

                if (options.withLimit) {
                    result = result.stream().skip(options.offset).limit(options.count).collect(toList());
                }
            }

            return convert(result);
        } catch (NumberFormatException e) {
            return error("ERR value is not an float or out of range");
        }
    }

    private Options parseOptions(Request request) {
        Options options = new Options();
        for (int i = 3; i < request.getLength(); i++) {
            String param = request.getParam(i).toString();
            if (param.equalsIgnoreCase(PARAM_LIMIT)) {
                options.withLimit = true;
                options.offset = parseInt(request.getParam(++i).toString());
                options.count = parseInt(request.getParam(++i).toString());
            } else if (param.equalsIgnoreCase(PARAM_WITHSCORES)) {
                options.withScores = true;
            }
        }
        return options;
    }

    private boolean inclusive(SafeString param) {
        return !param.toString().startsWith(EXCLUSIVE);
    }

    private float parseRange(String param) {
        switch (param) {
            case INIFITY:
                return Float.MAX_VALUE;
            case MINUS_INFINITY:
                return Float.MIN_VALUE;
            default:
                if (param.startsWith(EXCLUSIVE)) {
                    return Float.parseFloat(param.substring(1));
                }
                return Float.parseFloat(param);
        }
    }

    private static class Options {
        private boolean withScores;
        private boolean withLimit;
        private int offset;
        private int count;
    }
}
