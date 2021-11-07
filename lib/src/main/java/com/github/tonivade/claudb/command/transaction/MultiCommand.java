/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.transaction;

import com.github.tonivade.claudb.TransactionState;
import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.command.annotation.TxIgnore;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.command.Session;
import com.github.tonivade.resp.protocol.RedisToken;

import static com.github.tonivade.resp.protocol.RedisToken.error;
import static com.github.tonivade.resp.protocol.RedisToken.responseOk;

/**
 * @author zhou <br/>
 * <p>
 * redis 事务的 multi 命令实现。
 */
@Command("multi")
@TxIgnore
public class MultiCommand implements DBCommand {

    private static final String TRASACTION_KEY = "tx";

    /**
     * 命令形式： multi 标记一个事务块的开始。 随后的指令将在执行EXEC时作为一个原子执行。
     * @param db      当前数据库
     * @param request 命令请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        if (!isTxActive(request.getSession())) {
            createTransaction(request.getSession());
            return responseOk();
        } else {
            return error("ERR MULTI calls can not be nested");
        }
    }

    /**
     * 创建事务
     * @param session
     */
    private void createTransaction(Session session) {
        session.putValue(TRASACTION_KEY, new TransactionState());
    }

    /**
     * 判断事务是否激活
     * @param session
     * @return
     */
    private boolean isTxActive(Session session) {
        return session.getValue(TRASACTION_KEY).isPresent();
    }
}
