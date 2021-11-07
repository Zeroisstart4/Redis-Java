/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.data;

import com.github.tonivade.claudb.DBConfig;
import com.github.tonivade.claudb.DBServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author zhou <br/>
 * <p>
 * 数据库清理器
 */
public class DatabaseCleaner {

    /**
     * 日志
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseCleaner.class);
    /**
     * 数据库服务器上下文
     */
    private final DBServerContext server;
    /**
     * 数据库配置
     */
    private final DBConfig config;
    /**
     * 单例执行器
     */
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public DatabaseCleaner(DBServerContext server, DBConfig config) {
        this.server = server;
        this.config = config;
    }

    /**
     * 开启数据库清理
     */
    public void start() {
        executor.scheduleWithFixedDelay(this::clean,
                config.getCleanPeriod(), config.getCleanPeriod(), TimeUnit.SECONDS);
    }

    /**
     * 关闭数据库清理
     */
    public void stop() {
        executor.shutdown();
    }

    /**
     * 数据库清理
     */
    private void clean() {
        LOGGER.debug("cleaning database: running");
        server.clean(Instant.now());
        LOGGER.debug("cleaning database: done");
    }
}
