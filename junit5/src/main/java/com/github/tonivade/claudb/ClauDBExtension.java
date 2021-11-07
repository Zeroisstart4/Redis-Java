/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb;

import com.github.tonivade.resp.RespServer;
import org.junit.jupiter.api.extension.*;

import java.util.function.IntSupplier;


/**
 * @author zhou
 * <p>
 * Redis 数据库扩展
 */
public class ClauDBExtension implements BeforeAllCallback, AfterAllCallback, ParameterResolver {

    /**
     * 响应服务
     */
    private final RespServer server;

    public ClauDBExtension() {
        this.server = ClauDB.builder().randomPort().build();
    }

    /**
     * 前置任务
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        server.start();
    }

    /**
     * 后置任务
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        server.stop();
    }

    /**
     * 参数解析
     *
     * @param parameterContext
     * @param extensionContext
     * @return
     * @throws ParameterResolutionException
     */
    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return serverPort();
    }

    /**
     * 支持的参数校验
     *
     * @param parameterContext
     * @param extensionContext
     * @return
     * @throws ParameterResolutionException
     */
    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        Class<?> type = parameterContext.getParameter().getType();
        return IntSupplier.class.equals(type);
    }

    private IntSupplier serverPort() {
        return server::getPort;
    }
}
