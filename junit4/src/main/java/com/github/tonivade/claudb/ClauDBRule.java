/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb;

import com.github.tonivade.resp.RespServer;
import org.junit.rules.ExternalResource;

import static com.github.tonivade.purefun.Precondition.checkNonNull;

/**
 * @author zhou
 * <p>
 * Redis 简易规则
 */
public class ClauDBRule extends ExternalResource {

    /**
     * 响应服务
     */
    private final RespServer server;

    protected ClauDBRule(RespServer server) {
        this.server = checkNonNull(server);
    }

    /**
     * 获取域名 IP
     *
     * @return
     */
    public String getHost() {
        return server.getHost();
    }

    /**
     * 获取端口号
     *
     * @return
     */
    public int getPort() {
        return server.getPort();
    }

    /**
     * 前置任务
     *
     * @throws Throwable
     */
    @Override
    protected void before() throws Throwable {
        server.start();
    }

    /**
     * 后置任务
     */
    @Override
    protected void after() {
        server.stop();
    }

    /**
     * 以默认端口模式启动
     *
     * @return
     */
    public static ClauDBRule defaultPort() {
        return new ClauDBRule(ClauDB.builder().build());
    }

    /**
     * 以指定端口模式启动
     *
     * @param port 指定端口号
     * @return
     */
    public static ClauDBRule port(int port) {
        return new ClauDBRule(ClauDB.builder().port(port).build());
    }

    /**
     * 以随机端口模式启动
     *
     * @return
     */
    public static ClauDBRule randomPort() {
        return new ClauDBRule(ClauDB.builder().randomPort().build());
    }
}
