/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command;

import com.github.tonivade.claudb.DBServerContext;
import com.github.tonivade.purefun.data.ImmutableArray;
import com.github.tonivade.purefun.data.Sequence;
import com.github.tonivade.resp.command.*;
import com.github.tonivade.resp.protocol.AbstractRedisToken.ArrayRedisToken;
import com.github.tonivade.resp.protocol.AbstractRedisToken.StringRedisToken;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.RedisTokenVisitor;
import com.github.tonivade.resp.protocol.SafeString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.github.tonivade.resp.protocol.RedisToken.nullString;
import static com.github.tonivade.resp.protocol.RedisToken.visit;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * @author zhou <br/>
 * <p>
 * 数据库命令处理器
 */
public class DBCommandProcessor {

    /**
     * 日志
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(DBCommandProcessor.class);
    /**
     * 数据库服务器上下文
     */
    private final DBServerContext server;
    /**
     * 会话，用于记录请求的信息
     */
    private final Session session;

    public DBCommandProcessor(DBServerContext server) {
        this(server, new DefaultSession("dummy", null));
    }

    public DBCommandProcessor(DBServerContext server, Session session) {
        this.server = requireNonNull(server);
        this.session = requireNonNull(session);
    }

    /**
     * 进程命令
     *
     * @param token
     */
    public void processCommand(ArrayRedisToken token) {

        // 获取命令队列数组
        Sequence<RedisToken> array = token.getValue();
        // 获取请求 token
        StringRedisToken commandToken = (StringRedisToken) array.stream().findFirst().orElse(nullString());
        // 获取请求参数 token
        List<RedisToken> paramTokens = array.stream().skip(1).collect(toList());

        LOGGER.debug("new command recieved: {}", commandToken);
        // 通过 commandToken.getValue().toString() 获取请求类型
        RespCommand command = server.getCommand(commandToken.getValue().toString());
        // 为请求注入参数并执行
        if (command != null) {
            command.execute(request(commandToken, paramTokens));
        }
    }

    /**
     * 命令请求
     *
     * @param commandToken
     * @param array
     * @return
     */
    private Request request(StringRedisToken commandToken, List<RedisToken> array) {
        return new DefaultRequest(server, session, commandToken.getValue(), arrayToList(array));
    }

    /**
     * 类型转换
     *
     * @param request
     * @return
     */
    private ImmutableArray<SafeString> arrayToList(List<RedisToken> request) {
        RedisTokenVisitor<SafeString> visitor = RedisTokenVisitor.<SafeString>builder()
                .onString(StringRedisToken::getValue).build();
        return ImmutableArray.from(visit(request.stream(), visitor));
    }
}
