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
 * redis Pub/Sub 类型的 punsubscribe 命令实现。
 */
@ReadOnly
@Command("punsubscribe")
@ParamLength(1)
@PubSubAllowed
public class PatternUnsubscribeCommand implements DBCommand, PatternSubscriptionSupport {

    private static final String PUNSUBSCRIBE = "punsubscribe";

    /**
     * 命令形式： punsubscribe [pattern [pattern ...]] 指示客户端退订指定模式，若果没有提供模式则退出所有模式。
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
        for (SafeString channel : request.getParams()) {
            removePatternSubscription(admin, sessionId, channel);
            getSessionState(request.getSession()).removeSubscription(channel);
            result.addAll(asList(PUNSUBSCRIBE, channel, --i));
        }
        return convert(result);
    }

    private String getSessionId(Request request) {
        return request.getSession().getId();
    }

    private Sequence<SafeString> getChannels(Request request) {
        return getSessionState(request.getSession()).getSubscriptions();
    }
}
