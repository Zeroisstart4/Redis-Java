/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command;

import com.github.tonivade.claudb.data.DatabaseValue;
import com.github.tonivade.purefun.Pattern1;
import com.github.tonivade.purefun.data.ImmutableList;
import com.github.tonivade.purefun.data.ImmutableMap;
import com.github.tonivade.purefun.data.ImmutableSet;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.SafeString;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.stream.Stream;

import static com.github.tonivade.purefun.Matcher1.instanceOf;
import static com.github.tonivade.resp.protocol.RedisToken.array;
import static com.github.tonivade.resp.protocol.RedisToken.nullString;
import static java.util.stream.Collectors.toList;

/**
 * @author zhou <br/>
 * <p>
 * 数据库响应类
 */
class DBResponse {

    /**
     * 将 DatabaseValue 转为相应的数据类型       <br/>
     * <li> STRING                      <br/>
     * <li> HASH                        <br/>
     * <li> LIST                        <br/>
     * <li> SET                         <br/>
     * <li> ZSET                        <br/>
     * <li> 以及其他数据类型
     *
     * @param value
     * @return
     */
    static RedisToken convertValue(DatabaseValue value) {
        if (value != null) {
            switch (value.getType()) {
                case STRING:
                    SafeString string = value.getString();
                    return RedisToken.string(string);
                case HASH:
                    ImmutableMap<SafeString, SafeString> map = value.getHash();
                    return array(keyValueList(map).toList());
                case LIST:
                    ImmutableList<SafeString> list = value.getList();
                    return convertArray(list.toList());
                case SET:
                    ImmutableSet<SafeString> set = value.getSet();
                    return convertArray(set.toSet());
                case ZSET:
                    NavigableSet<Entry<Double, SafeString>> zset = value.getSortedSet();
                    return convertArray(serialize(zset));
                default:
                    break;
            }
        }
        return RedisToken.nullString();
    }

    /**
     * 将 array 集合转为 RedisToken
     *
     * @param array
     * @return
     */
    static RedisToken convertArray(Collection<?> array) {
        if (array == null) {
            return RedisToken.array();
        }
        return RedisToken.array(array.stream().map(DBResponse::parseToken).collect(toList()));
    }

    /**
     * 解析 value 转为相应的数据类型的 RedisToken
     *
     * @param value
     * @return
     */
    private static RedisToken parseToken(Object value) {
        return Pattern1.<Object, RedisToken>build()
                .when(instanceOf(Integer.class))
                .then(integer -> RedisToken.integer((Integer) integer))
                .when(instanceOf(Boolean.class))
                .then(bool -> RedisToken.integer((Boolean) bool))
                .when(instanceOf(String.class))
                .then(string -> RedisToken.string((String) string))
                .when(instanceOf(Double.class))
                .then(double_ -> RedisToken.string(double_.toString()))
                .when(instanceOf(SafeString.class))
                .then(string -> RedisToken.string((SafeString) string))
                .when(instanceOf(DatabaseValue.class))
                .then(value_ -> DBResponse.convertValue((DatabaseValue) value_))
                .when(instanceOf(RedisToken.class))
                .then(token -> (RedisToken) token)
                .otherwise()
                .returns(nullString())
                .apply(value);
    }

    /**
     * 将 ImmutableMap 转为 ImmutableList
     *
     * @param map
     * @return
     */
    private static ImmutableList<RedisToken> keyValueList(ImmutableMap<SafeString, SafeString> map) {
        return ImmutableList.from(map.entries().stream()
                .flatMap(entry -> Stream.of(entry.get1(), entry.get2()))
                .map(RedisToken::string));
    }

    private static Collection<?> serialize(NavigableSet<Entry<Double, SafeString>> set) {
        return set.stream()
                .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue())).collect(toList());
    }
}
