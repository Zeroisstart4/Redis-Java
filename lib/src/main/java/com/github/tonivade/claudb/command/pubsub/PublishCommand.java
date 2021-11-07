/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.pubsub;

import com.github.tonivade.claudb.DBServerContext;
import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.SafeString;

import static com.github.tonivade.resp.protocol.RedisToken.integer;

/**
 * @author zhou <br/>
 *
 * redis Pub/Sub 类型的 publish 命令实现。
 */
@Command("publish")
@ParamLength(2)
public class PublishCommand implements DBCommand, SubscriptionSupport, PatternSubscriptionSupport {

    /**
     * 命令形式： publish channel message 将信息 message 发送到指定的频道 channel
     * @param db      当前数据库
     * @param request 命令请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        String channel = request.getParam(0).toString();
        SafeString message = request.getParam(1);
        return integer(publishAll(getClauDB(request.getServerContext()), channel, message));
    }

    /**
     * 将消息message 发送到指定的频道 channel
     * @param server        数据库服务器上下文
     * @param channel       订阅管道
     * @param message       订阅消息
     * @return
     */
    private int publishAll(DBServerContext server, String channel, SafeString message) {
        int count = publish(server, channel, message);
        int pcount = patternPublish(server, channel, message);
        return count + pcount;
    }
}
