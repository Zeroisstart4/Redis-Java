/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb;

import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.purefun.data.ImmutableList;
import com.github.tonivade.resp.command.ServerContext;
import com.github.tonivade.resp.protocol.RedisToken;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;


/**
 * @author zhou <br/>
 * <p>
 * 数据库服务器上下文
 */
public interface DBServerContext extends ServerContext {

    /**
     * 默认端口
     */
    int DEFAULT_PORT = 7081;
    /**
     * 默认 IP
     */
    String DEFAULT_HOST = "localhost";

    /**
     * 是否为主库
     *
     * @return
     */
    boolean isMaster();

    /**
     * 设置主库
     *
     * @param master
     */
    void setMaster(boolean master);

    /**
     * 导入 RDB
     *
     * @param input
     * @throws IOException
     */
    void importRDB(InputStream input) throws IOException;

    /**
     * 导出 RDB
     *
     * @param output
     * @throws IOException
     */
    void exportRDB(OutputStream output) throws IOException;

    /**
     * 切换数据库
     *
     * @param i
     * @return
     */
    Database getDatabase(int i);

    /**
     * 获取主库
     *
     * @return
     */
    Database getAdminDatabase();

    /**
     * 发布消息
     *
     * @param sourceKey
     * @param message
     */
    void publish(String sourceKey, RedisToken message);

    /**
     * 获取命令集合
     *
     * @return
     */
    ImmutableList<RedisToken> getCommandsToReplicate();

    /**
     * 垃圾清理
     *
     * @param now
     */
    void clean(Instant now);
}
