/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.data;

import com.github.tonivade.purefun.Equal;
import com.github.tonivade.resp.protocol.SafeString;

import java.io.Serializable;
import java.util.Objects;

import static com.github.tonivade.resp.protocol.SafeString.safeString;

/**
 * @author zhou <br/>
 * <p>
 * 数据库键
 */
public class DatabaseKey implements Comparable<DatabaseKey>, Serializable {

    /**
     * 序列化 UID
     */
    private static final long serialVersionUID = 7710472090270782053L;
    /**
     * 比较器
     */
    private static final Equal<DatabaseKey> EQUAL = Equal.<DatabaseKey>of().comparing(k -> k.value);
    /**
     * 数据库值
     */
    private final SafeString value;

    public DatabaseKey(SafeString value) {
        this.value = value;
    }

    public SafeString getValue() {
        return value;
    }

    @Override
    public int compareTo(DatabaseKey o) {
        return value.compareTo(o.getValue());
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public boolean equals(Object obj) {
        return EQUAL.applyTo(this, obj);
    }

    @Override
    public String toString() {
        return value.toString();
    }

    /**
     * 将安全类型包装为数据库键类型
     * @param str
     * @return
     */
    public static DatabaseKey safeKey(SafeString str) {
        return new DatabaseKey(str);
    }

    /**
     * 将普通 String 包装为 SafeString 类型，再包装为数据库键
     * @param str
     * @return
     */
    public static DatabaseKey safeKey(String str) {
        return safeKey(safeString(str));
    }
}
