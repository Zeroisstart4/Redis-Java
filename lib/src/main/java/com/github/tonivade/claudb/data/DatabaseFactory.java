/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.data;

/**
 * @author zhou <br/>
 * <p>
 * 数据库工厂
 */
public interface DatabaseFactory {

    /**
     * 创建数据库
     * @param name  数据库名
     * @return
     */
    Database create(String name);

    /**
     * 清理数据库
     */
    void clear();
}
