/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.pubsub;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.command.annotation.PubSubAllowed;
import com.github.tonivade.claudb.command.annotation.ReadOnly;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.purefun.data.Sequence;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.SafeString;

import java.util.LinkedList;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * @author zhou <br/>
 * <p>
 * redis Pub/Sub 类型的 psubscribe 命令实现。
 */
@ReadOnly
@Command("psubscribe")
@ParamLength(1)
@PubSubAllowed
public class PatternSubscribeCommand implements DBCommand, PatternSubscriptionSupport {

    /**
     * 订阅给定的模式
     */
    private static final String PSUBSCRIBE = "psubscribe";

    /**
     * 命令形式： psubscribe pattern [pattern ...] 订阅给定的模式(patterns)。
     * @param db      当前数据库
     * @param request 命令请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        Database admin = getAdminDatabase(request.getServerContext());
        String sessionId = getSessionId(request);
        Sequence<SafeString> channels = getChannels(request);
        int i = channels.size();
        List<Object> result = new LinkedList<>();
        for (SafeString pattern : request.getParams()) {
            addPatternSubscription(admin, sessionId, pattern);
            getSessionState(request.getSession()).addSubscription(pattern);
            result.addAll(asList(PSUBSCRIBE, pattern, ++i));
        }
        return convert(result);
    }

    /**
     * 获取请求的 SessionId
     * @param request
     * @return
     */
    private String getSessionId(Request request) {
        return request.getSession().getId();
    }

    /**
     * 获取订阅管道
     * @param request
     * @return
     */
    private Sequence<SafeString> getChannels(Request request) {
        return getSessionState(request.getSession()).getSubscriptions();
    }
}
