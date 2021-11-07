/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.pubsub;

import com.github.tonivade.claudb.DBServerContext;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.purefun.data.ImmutableSet;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.SafeString;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.claudb.data.DatabaseValue.set;
import static com.github.tonivade.resp.protocol.SafeString.safeString;

/**
 * @author zhou <br/>
 * <p>
 * 基本订阅支持接口
 */
public interface BaseSubscriptionSupport {

    /**
     * 添加订阅
     * @param suffix        字符前缀
     * @param admin         订阅数据库
     * @param sessionId     会话 id
     * @param channel       订阅管道
     */
    default void addSubscription(String suffix, Database admin, String sessionId, SafeString channel) {
        admin.merge(safeKey(suffix + channel), set(safeString(sessionId)),
                (oldValue, newValue) -> set(oldValue.getSet().appendAll(newValue.getSet())));
    }

    /**
     * 删除订阅
     * @param suffix        字符前缀
     * @param admin         订阅数据库
     * @param sessionId     会话 id
     * @param channel       订阅管道
     */
    default void removeSubscription(String suffix, Database admin, String sessionId, SafeString channel) {
        admin.merge(safeKey(suffix + channel), set(safeString(sessionId)),
                (oldValue, newValue) -> set(oldValue.getSet().removeAll(newValue.getSet())));
    }

    /**
     * 发布订阅
     * @param server        数据库服务器上下文
     * @param clients       客户端
     * @param message       订阅信息
     * @return
     */
    default int publish(DBServerContext server, ImmutableSet<SafeString> clients, RedisToken message) {
        clients.forEach(client -> server.publish(client.toString(), message));
        return clients.size();
    }
}
