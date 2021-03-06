/*
 * Copyright (c) 2020, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.data;

import java.util.HashMap;

/**
 * @author zhou <br/>
 * <p>
 * 堆上数据库工厂
 */
public class OnHeapDatabaseFactory implements DatabaseFactory {

    /**
     * 创建数据库
     * @param name  数据库名
     * @return
     */
    @Override
    public Database create(String name) {
        return new OnHeapDatabase(new HashMap<>());
    }

    /**
     * 清理数据库
     */
    @Override
    public void clear() {
        // nothing to clear
    }
}
