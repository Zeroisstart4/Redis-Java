/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.transaction;

import com.github.tonivade.claudb.TransactionState;
import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.command.annotation.TxIgnore;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.purefun.type.Option;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.command.Session;
import com.github.tonivade.resp.protocol.RedisToken;

/**
 * @author zhou <br/>
 * <p>
 * redis 事务的 discard 命令实现。
 */
@Command("discard")
@TxIgnore
public class DiscardCommand implements DBCommand {

    private static final String TX_KEY = "tx";

    /**
     * 命令形式： discard 刷新一个事务中所有在排队等待的指令，中止事务
     *
     * @param db      当前数据库
     * @param request 命令请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        removeTransactionIfExists(request.getSession());

        return RedisToken.responseOk();
    }

    private Option<TransactionState> removeTransactionIfExists(Session session) {
        return session.removeValue(TX_KEY);
    }
}
