/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb;

import com.github.tonivade.claudb.command.DBCommandSuite;
import com.github.tonivade.claudb.data.*;
import com.github.tonivade.claudb.event.Event;
import com.github.tonivade.claudb.event.NotificationManager;
import com.github.tonivade.claudb.persistence.PersistenceManager;
import com.github.tonivade.purefun.Recoverable;
import com.github.tonivade.purefun.data.ImmutableArray;
import com.github.tonivade.purefun.data.ImmutableList;
import com.github.tonivade.purefun.type.Option;
import com.github.tonivade.resp.RespServer;
import com.github.tonivade.resp.RespServerContext;
import com.github.tonivade.resp.SessionListener;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.command.RespCommand;
import com.github.tonivade.resp.command.Session;
import com.github.tonivade.resp.protocol.RedisToken;
import io.reactivex.rxjava3.core.Observable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.time.Instant;

import static com.github.tonivade.purefun.data.Sequence.listOf;
import static com.github.tonivade.resp.protocol.RedisToken.error;
import static com.github.tonivade.resp.protocol.SafeString.safeString;
import static java.lang.String.valueOf;

/**
 * @author zhou <br/>
 * <p>
 * redis 数据库
 */
public final class ClauDB extends RespServerContext implements DBServerContext {

    /**
     * 数据库状态
     */
    private static final String STATE = "state";
    /**
     * 日志
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ClauDB.class);
    /**
     * 数据清理器
     */
    private DatabaseCleaner cleaner;
    /**
     * 数据持久化
     */
    private Option<PersistenceManager> persistence;
    /**
     * 消息通知
     */
    private Option<NotificationManager> notifications;
    /**
     * 数据库配置
     */
    private final DBConfig config;

    public ClauDB() {
        this(DEFAULT_HOST, DEFAULT_PORT);
    }

    public ClauDB(String host, int port) {
        this(host, port, DBConfig.builder().build());
    }

    public ClauDB(String host, int port, DBConfig config) {
        super(host, port, new DBCommandSuite(), new DBSessionListener());
        this.config = config;
    }

    /**
     * 构建响应请求
     *
     * @return
     */
    public static ClauDB.Builder builder() {
        return new Builder();
    }

    /**
     * 启动服务器
     */
    @Override
    public void start() {
        super.start();

        init();

        getState().setMaster(true);

        persistence.ifPresent(PersistenceManager::start);
        notifications.ifPresent(NotificationManager::start);
        cleaner.start();
    }

    /**
     * 关闭服务器
     */
    @Override
    public void stop() {
        persistence.ifPresent(PersistenceManager::stop);
        notifications.ifPresent(NotificationManager::stop);
        cleaner.stop();

        getState().clear();

        persistence = null;
        notifications = null;
        cleaner = null;

        super.stop();
    }

    /**
     * 获取命令集合
     *
     * @return
     */
    @Override
    public ImmutableList<RedisToken> getCommandsToReplicate() {
        return executeOn(Observable.<ImmutableList<RedisToken>>create(observable -> {
            observable.onNext(getState().getCommandsToReplicate());
            observable.onComplete();
        })).blockingFirst();
    }

    /**
     * 发送消息
     *
     * @param sourceKey
     * @param message
     */
    @Override
    public void publish(String sourceKey, RedisToken message) {
        Session session = getSession(sourceKey);
        if (session != null) {
            session.publish(message);
        }
    }

    /**
     * 获取当前数据库
     *
     * @return
     */
    @Override
    public Database getAdminDatabase() {
        return getState().getAdminDatabase();
    }

    /**
     * 模拟 Redis 16 个数据库的切换
     *
     * @param i 数据库的索引
     * @return
     */
    @Override
    public Database getDatabase(int i) {
        return getState().getDatabase(i);
    }

    /**
     * 导出 RDB 文件
     *
     * @param output 输出流
     */
    @Override
    public void exportRDB(OutputStream output) {
        executeOn(Observable.create(observable -> {
            getState().exportRDB(output);
            observable.onComplete();
        })).blockingSubscribe();
    }

    /**
     * 导入 RDB 文件
     *
     * @param input 输入流
     */
    @Override
    public void importRDB(InputStream input) {
        executeOn(Observable.create(observable -> {
            getState().importRDB(input);
            observable.onComplete();
        })).blockingSubscribe();
    }

    /**
     * 是否为主库
     *
     * @return
     */
    @Override
    public boolean isMaster() {
        return getState().isMaster();
    }

    /**
     * 设置为主库
     *
     * @param master
     */
    @Override
    public void setMaster(boolean master) {
        getState().setMaster(master);
    }

    /**
     * 过期数据清理
     *
     * @param now 当前时刻的时间戳
     */
    @Override
    public void clean(Instant now) {
        executeOn(Observable.create(observable -> {
            getState().evictExpired(now);
            observable.onComplete();
        })).blockingSubscribe();
    }

    /**
     * 执行 Redis 命令，并返回响应
     *
     * @param command Redis 命令
     * @param request Redis 请求
     * @return
     */
    @Override
    protected RedisToken executeCommand(RespCommand command, Request request) {
        if (!isReadOnly(request.getCommand())) {
            try {
                RedisToken response = command.execute(request);
                replication(request);
                notification(request);
                return response;
            } catch (RuntimeException e) {
                LOGGER.error("error executing command: " + request, e);
                return error("error executing command: " + request);
            }
        } else {
            return error("READONLY You can't write against a read only slave");
        }
    }

    private boolean isReadOnly(String command) {
        return !isMaster() && !isReadOnlyCommand(command);
    }

    /**
     * 进行主从复制
     *
     * @param request 主从复制请求
     */
    private void replication(Request request) {
        if (!isReadOnlyCommand(request.getCommand())) {
            RedisToken array = requestToArray(request);
            if (hasSlaves()) {
                getState().append(array);
            }
            persistence.ifPresent(manager -> manager.append(array));
        }
    }

    /**
     * 进行事件通知
     *
     * @param request
     */
    private void notification(Request request) {
        if (!isReadOnlyCommand(request.getCommand()) && request.getLength() > 1) {
            notifications.ifPresent(manager -> publishEvent(manager, request));
        }
    }

    /**
     * 是否为只读命令
     *
     * @param command Redis 命令
     * @return
     */
    private boolean isReadOnlyCommand(String command) {
        return getDBCommands().isReadOnly(command);
    }

    /**
     * 发布事件
     *
     * @param manager 通知管理器
     * @param request Redis 请求
     */
    private void publishEvent(NotificationManager manager, Request request) {
        manager.enqueue(createKeyEvent(request));
        manager.enqueue(createCommandEvent(request));
    }

    /**
     * 创建 Key 事件
     *
     * @param request Redis 请求
     * @return
     */
    private Event createKeyEvent(Request request) {
        return Event.keyEvent(safeString(request.getCommand()), request.getParam(0), currentDB(request));
    }

    /**
     * 创建命令事件
     *
     * @param request Redis 请求
     * @return
     */
    private Event createCommandEvent(Request request) {
        return Event.commandEvent(safeString(request.getCommand()), request.getParam(0), currentDB(request));
    }

    /**
     * 获取当前数据库
     *
     * @param request Redis 请求
     * @return
     */
    private Integer currentDB(Request request) {
        return getSessionState(request.getSession()).getCurrentDB();
    }

    /**
     * 将 Redis 请求转为数组
     *
     * @param request Redis 请求
     * @return
     */
    private RedisToken requestToArray(Request request) {
        return RedisToken.array(listOf(currentDbToken(request))
                .append(commandToken(request))
                .appendAll(paramTokens(request)));
    }

    /**
     * 将 Redis 命令转为 String 类型
     *
     * @param request Redis 请求
     * @return
     */
    private RedisToken commandToken(Request request) {
        return RedisToken.string(request.getCommand());
    }

    /**
     * 将当前数据库转为 String 类型
     *
     * @param request Redis 请求
     * @return
     */
    private RedisToken currentDbToken(Request request) {
        return RedisToken.string(valueOf(getCurrentDB(request)));
    }

    /**
     * 获取当前数据库序号
     *
     * @param request Redis 请求
     * @return
     */
    private int getCurrentDB(Request request) {
        return getSessionState(request.getSession()).getCurrentDB();
    }

    /**
     * 将 Redis 请求参数转为 String 类型，并存入不可变数组
     *
     * @param request
     * @return
     */
    private ImmutableArray<RedisToken> paramTokens(Request request) {
        return request.getParams().map(RedisToken::string);
    }

    /**
     * 数据库 Session 状态
     *
     * @param session Session 会话
     * @return
     */
    private DBSessionState getSessionState(Session session) {
        return sessionState(session).getOrElseThrow(() -> new IllegalStateException("missing session state"));
    }

    /**
     * Session 状态
     *
     * @param session Session 会话
     * @return
     */
    private Option<DBSessionState> sessionState(Session session) {
        return session.getValue(STATE);
    }

    /**
     * 服务器状态
     *
     * @return
     */
    private DBServerState getState() {
        return serverState().getOrElseThrow(() -> new IllegalStateException("missing server state"));
    }

    /**
     * 服务器状态
     *
     * @return
     */
    private Option<DBServerState> serverState() {
        return getValue(STATE);
    }

    /**
     * 是否有从节点
     *
     * @return
     */
    private boolean hasSlaves() {
        return getState().hasSlaves();
    }

    /**
     * 获取数据库命令
     *
     * @return
     */
    private DBCommandSuite getDBCommands() {
        return (DBCommandSuite) getCommands();
    }

    /**
     * 数据库初始化
     */
    private void init() {
        DatabaseFactory factory = initFactory();

        putValue(STATE, new DBServerState(factory, config.getNumDatabases()));

        initPersistence();
        initNotifications();
        initCleaner();
    }

    /**
     * 初始化垃圾清理器
     */
    private void initCleaner() {
        this.cleaner = new DatabaseCleaner(this, config);
    }

    /**
     * 初始化事件通知
     */
    private void initNotifications() {
        if (config.isNotificationsActive()) {
            this.notifications = Option.some(new NotificationManager(this));
        } else {
            this.notifications = Option.none();
        }
    }

    /**
     * 初始化数据持久化
     */
    private void initPersistence() {
        if (config.isPersistenceActive()) {
            this.persistence = Option.some(new PersistenceManager(this, config));
        } else {
            this.persistence = Option.none();
        }
    }

    /**
     * 初始化数据库工厂
     *
     * @return
     */
    private DatabaseFactory initFactory() {
        DatabaseFactory factory;
        if (config.isOffHeapActive()) {
            factory = new OffHeapDatabaseFactory();
        } else {
            factory = new OnHeapDatabaseFactory();
        }
        return factory;
    }

    /**
     * 数据库 Session 监听器
     */
    private static final class DBSessionListener implements SessionListener {

        /**
         * 删除会话
         *
         * @param session Session 会话
         */
        @Override
        public void sessionDeleted(Session session) {
            session.destroy();
        }

        /**
         * 创建 Session
         *
         * @param session Session 会话
         */
        @Override
        public void sessionCreated(Session session) {
            session.putValue(STATE, new DBSessionState());
        }
    }

    /**
     * 数据库构建器
     */
    public static class Builder implements Recoverable {
        private String host = DEFAULT_HOST;
        private int port = DEFAULT_PORT;
        private DBConfig config = DBConfig.builder().build();

        /**
         * 指定域名 IP
         *
         * @param host
         * @return
         */
        public Builder host(String host) {
            this.host = host;
            return this;
        }

        /**
         * 指定端口
         *
         * @param port
         * @return
         */
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /**
         * 随机端口
         *
         * @return
         */
        public Builder randomPort() {
            try (ServerSocket socket = new ServerSocket(0)) {
                socket.setReuseAddress(true);
                this.port = socket.getLocalPort();
            } catch (IOException e) {
                return sneakyThrow(e);
            }
            return this;
        }

        /**
         * 数据库配置
         *
         * @param config 数据库配置信息
         * @return
         */
        public Builder config(DBConfig config) {
            this.config = config;
            return this;
        }

        /**
         * 创建响应服务
         *
         * @return
         */
        public RespServer build() {
            return new RespServer(new ClauDB(host, port, config));
        }
    }
}
