/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.replication;

import com.github.tonivade.claudb.DBServerContext;
import com.github.tonivade.claudb.DBServerState;
import com.github.tonivade.purefun.type.Option;
import com.github.tonivade.resp.protocol.AbstractRedisToken.ArrayRedisToken;
import com.github.tonivade.resp.protocol.AbstractRedisTokenVisitor;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.SafeString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.github.tonivade.resp.protocol.RedisToken.array;
import static com.github.tonivade.resp.protocol.RedisToken.string;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * @author zhou <br/>
 * <p>
 * 主节点复制
 */
public class MasterReplication implements Runnable {

    /**
     * 日志
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(MasterReplication.class);
    /**
     * Select 命令
     */
    private static final String SELECT_COMMAND = "SELECT";
    /**
     * Ping 命令
     */
    private static final String PING_COMMAND = "PING";
    /**
     * 任务延迟时间
     */
    private static final int TASK_DELAY = 2;
    /**
     * 数据库服务器上下文
     */
    private final DBServerContext server;
    /**
     * 单例线程池
     */
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public MasterReplication(DBServerContext server) {
        this.server = requireNonNull(server);
    }

    /**
     * 开启主节点复制
     */
    public void start() {
        executor.scheduleWithFixedDelay(this, TASK_DELAY, TASK_DELAY, TimeUnit.SECONDS);
    }

    /**
     * 关闭主节点复制
     */
    public void stop() {
        executor.shutdown();
    }

    /**
     * 添加从节点
     * @param id
     */
    public void addSlave(String id) {
        getServerState().addSlave(id);
        LOGGER.info("new slave: {}", id);
    }

    /**
     * 移除从节点
     * @param id
     */
    public void removeSlave(String id) {
        getServerState().removeSlave(id);
        LOGGER.info("slave revomed: {}", id);
    }

    /**
     * 通过轮询的方式向每一个从节点发送复制数据
     */
    @Override
    public void run() {
        List<RedisToken> commands = createCommands();

        for (SafeString slave : getServerState().getSlaves()) {
            for (RedisToken command : commands) {
                server.publish(slave.toString(), command);
            }
        }
    }

    private List<RedisToken> createCommands() {
        List<RedisToken> commands = new LinkedList<>();
        commands.add(pingCommand());
        commands.addAll(commandsToReplicate());
        return commands;
    }

    private List<RedisToken> commandsToReplicate() {
        List<RedisToken> commands = new LinkedList<>();

        for (RedisToken command : server.getCommandsToReplicate()) {
            command.accept(new AbstractRedisTokenVisitor<Void>() {
                @Override
                public Void array(ArrayRedisToken token) {
                    commands.add(selectCommand(token));
                    commands.add(command(token));
                    return null;
                }
            });
        }
        return commands;
    }

    private RedisToken selectCommand(ArrayRedisToken token) {
        return array(string(SELECT_COMMAND),
                token.getValue().stream().findFirst().orElse(string("0")));
    }

    /**
     * 心跳检测
     * @return
     */
    private RedisToken pingCommand() {
        return array(string(PING_COMMAND));
    }

    private RedisToken command(ArrayRedisToken token) {
        return array(token.getValue().stream().skip(1).collect(toList()));
    }

    private DBServerState getServerState() {
        return serverState().getOrElseThrow(() -> new IllegalStateException("missing server state"));
    }

    private Option<DBServerState> serverState() {
        return server.getValue("state");
    }
}
