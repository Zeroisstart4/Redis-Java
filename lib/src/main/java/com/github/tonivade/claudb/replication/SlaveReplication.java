/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.replication;

import com.github.tonivade.claudb.DBServerContext;
import com.github.tonivade.claudb.command.DBCommandProcessor;
import com.github.tonivade.claudb.data.DatabaseKey;
import com.github.tonivade.claudb.data.DatabaseValue;
import com.github.tonivade.claudb.persistence.ByteBufferInputStream;
import com.github.tonivade.resp.RespCallback;
import com.github.tonivade.resp.RespClient;
import com.github.tonivade.resp.command.Session;
import com.github.tonivade.resp.protocol.AbstractRedisToken.StringRedisToken;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.RedisTokenVisitor;
import com.github.tonivade.resp.protocol.SafeString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.claudb.data.DatabaseValue.entry;
import static com.github.tonivade.claudb.data.DatabaseValue.hash;
import static com.github.tonivade.resp.protocol.RedisToken.array;
import static com.github.tonivade.resp.protocol.RedisToken.string;
import static com.github.tonivade.resp.protocol.SafeString.safeString;
import static java.lang.String.valueOf;
import static java.util.Objects.requireNonNull;

/**
 * @author zhou <br/>
 *
 * 从节点复制
 */
public class SlaveReplication implements RespCallback {

    /**
     * 主节点键
     */
    private static final DatabaseKey MASTER_KEY = safeKey("master");
    /**
     * 日志
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(SlaveReplication.class);
    /**
     * 同步命令
     */
    private static final String SYNC_COMMAND = "SYNC";
    /**
     * 客户端
     */
    private final RespClient client;
    /**
     * 数据库服务器上下文
     */
    private final DBServerContext server;
    /**
     * 数据库命令处理器
     */
    private final DBCommandProcessor processor;
    /**
     * 主机域名
     */
    private final String host;
    /**
     * 主机 IP
     */
    private final int port;

    public SlaveReplication(DBServerContext server, Session session, String host, int port) {
        this.server = requireNonNull(server);
        this.host = requireNonNull(host);
        this.port = port;
        this.client = new RespClient(host, port, this);
        this.processor = new DBCommandProcessor(server, session);
    }

    public void start() {
        client.start();
        server.setMaster(false);
        server.getAdminDatabase().put(MASTER_KEY, createState(false));
    }

    public void stop() {
        client.stop();
        server.setMaster(true);
    }

    @Override
    public void onConnect() {
        LOGGER.info("Connected with master");
        client.send(array(string(SYNC_COMMAND)));
        server.getAdminDatabase().put(MASTER_KEY, createState(true));
    }

    @Override
    public void onDisconnect() {
        LOGGER.info("Disconnected from master");
        server.getAdminDatabase().put(MASTER_KEY, createState(false));
    }

    @Override
    public void onMessage(RedisToken token) {
        token.accept(RedisTokenVisitor.builder()
                .onString(string -> {
                    processRDB(string);
                    return null;
                })
                .onArray(array -> {
                    processor.processCommand(array);
                    return null;
                }).build());
    }

    private void processRDB(StringRedisToken token) {
        try {
            SafeString value = token.getValue();
            server.importRDB(toStream(value));
            LOGGER.info("loaded RDB file from master");
        } catch (IOException e) {
            LOGGER.error("error importing RDB file", e);
        }
    }

    private InputStream toStream(SafeString value) {
        return new ByteBufferInputStream(value.getBytes());
    }

    private DatabaseValue createState(boolean connected) {
        return hash(entry(safeString("host"), safeString(host)),
                entry(safeString("port"), safeString(valueOf(port))),
                entry(safeString("state"), safeString(connected ? "connected" : "disconnected")));
    }
}
