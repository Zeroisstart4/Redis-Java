/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.data;

import com.github.tonivade.purefun.Tuple2;
import com.github.tonivade.purefun.data.ImmutableList;
import com.github.tonivade.purefun.data.ImmutableMap;
import com.github.tonivade.purefun.data.ImmutableSet;
import com.github.tonivade.purefun.data.Sequence;
import com.github.tonivade.resp.protocol.SafeString;

import java.time.Instant;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.function.BiFunction;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;

/**
 * @author zhou <br/>
 * <p>
 * 数据库接口
 */
public interface Database {

    /**
     * 数据库大小
     * @return
     */
    int size();

    /**
     * 数据库是否为空
     * @return
     */
    boolean isEmpty();

    /**
     * 是否含有 DatabaseKey
     * @param key   数据库键
     * @return
     */
    boolean containsKey(DatabaseKey key);

    /**
     * 通过 DatabaseKey 获取 DatabaseValue
     * @param key   数据库键
     * @return
     */
    DatabaseValue get(DatabaseKey key);

    /**
     * 存入一个数据
     * @param key       数据库键
     * @param value     数据库值
     * @return
     */
    DatabaseValue put(DatabaseKey key, DatabaseValue value);

    /**
     * 移除某一个 key
     * @param key   数据库键
     * @return
     */
    DatabaseValue remove(DatabaseKey key);

    /**
     * 清空数据库
     */
    void clear();

    /**
     * 获取键集合
     * @return
     */
    ImmutableSet<DatabaseKey> keySet();

    /**
     * 获取值集合
     * @return
     */
    Sequence<DatabaseValue> values();

    /**
     * 获取 entry 集合
     * @return
     */
    ImmutableSet<Tuple2<DatabaseKey, DatabaseValue>> entrySet();

    /**
     * 获取 String 类型
     * @param key   数据库键
     * @return
     */
    default SafeString getString(SafeString key) {
        return getOrDefault(safeKey(key), DatabaseValue.EMPTY_STRING).getString();
    }

    /**
     * 获取 List 类型
     * @param key   数据库键
     * @return
     */
    default ImmutableList<SafeString> getList(SafeString key) {
        return getOrDefault(safeKey(key), DatabaseValue.EMPTY_LIST).getList();
    }

    /**
     * 获取 Set 类型
     * @param key   数据库键
     * @return
     */
    default ImmutableSet<SafeString> getSet(SafeString key) {
        return getOrDefault(safeKey(key), DatabaseValue.EMPTY_SET).getSet();
    }

    /**
     * 获取 ZSet 类型
     * @param key   数据库键
     * @return
     */
    default NavigableSet<Entry<Double, SafeString>> getSortedSet(SafeString key) {
        return getOrDefault(safeKey(key), DatabaseValue.EMPTY_ZSET).getSortedSet();
    }

    /**
     * 获取 Hash 类型
     * @param key   数据库键
     * @return
     */
    default ImmutableMap<SafeString, SafeString> getHash(SafeString key) {
        return getOrDefault(safeKey(key), DatabaseValue.EMPTY_HASH).getHash();
    }

    /**
     * HashMap 类型的 putAll 方法重构
     * @param map   数据库 map
     */
    default void putAll(ImmutableMap<? extends DatabaseKey, ? extends DatabaseValue> map) {
        map.forEach(this::put);
    }

    /**
     * HashMap 类型的 putIfAbsent 方法重构
     * @param key       数据库键
     * @param value     数据库值
     * @return
     */
    default DatabaseValue putIfAbsent(DatabaseKey key, DatabaseValue value) {
        DatabaseValue oldValue = get(key);
        if (oldValue == null) {
            oldValue = put(key, value);
        }
        return oldValue;
    }

    /**
     * 数据合并
     * @param key                   数据库键
     * @param value                 数据库值
     * @param remappingFunction     函数接口
     * @return
     */
    default DatabaseValue merge(DatabaseKey key, DatabaseValue value,
                                BiFunction<DatabaseValue, DatabaseValue, DatabaseValue> remappingFunction) {
        DatabaseValue oldValue = get(key);
        DatabaseValue newValue = oldValue == null ? value : remappingFunction.apply(oldValue, value);
        if (newValue == null) {
            remove(key);
        } else {
            put(key, newValue);
        }
        return newValue;
    }

    /**
     * HashMap 类型的 getOrDefault 方法重构
     * @param key               数据库键
     * @param defaultValue      默认数据库值
     * @return
     */
    default DatabaseValue getOrDefault(DatabaseKey key, DatabaseValue defaultValue) {
        DatabaseValue value = get(key);
        return (value != null || containsKey(key)) ? value : defaultValue;
    }

    /**
     * 判断键类型
     * @param key       数据库键
     * @param type      预测类型
     * @return
     */
    default boolean isType(DatabaseKey key, DataType type) {
        DatabaseValue value = get(key);
        return value != null ? value.getType() == type : true;
    }

    /**
     * 数据库键重命名
     * @param from      旧数据库键名
     * @param to        新数据库键名
     * @return
     */
    default boolean rename(DatabaseKey from, DatabaseKey to) {
        DatabaseValue value = remove(from);
        if (value != null) {
            put(to, value);
            return true;
        }
        return false;
    }

    /**
     * 覆盖全部
     * @param value
     */
    default void overrideAll(ImmutableMap<DatabaseKey, DatabaseValue> value) {
        clear();
        putAll(value);
    }

    /**
     * 逐出键集合
     * @param now
     * @return
     */
    default ImmutableSet<DatabaseKey> evictableKeys(Instant now) {
        return entrySet()
                .filter(entry -> entry.get2().isExpired(now))
                .map(Tuple2::get1);
    }
}
