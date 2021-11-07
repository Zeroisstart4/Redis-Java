/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.transaction;

import com.github.tonivade.claudb.DBServerContext;
import com.github.tonivade.claudb.TransactionState;
import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.command.annotation.TxIgnore;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.purefun.type.Option;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.command.RespCommand;
import com.github.tonivade.resp.command.Session;
import com.github.tonivade.resp.protocol.RedisToken;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhou <br/>
 * <p>
 * redis 事务的 exec 命令实现。
 */
@Command("exec")
@TxIgnore
public class ExecCommand implements DBCommand {

    /**
     * 命令形式： exec 执行事务中所有在排队等待的指令
     *
     * @param db      当前数据库
     * @param request 命令请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        Option<TransactionState> transaction = getTransactionIfExists(request.getSession());
        if (transaction.isPresent()) {
            DBServerContext server = getClauDB(request.getServerContext());
            List<RedisToken> responses = new ArrayList<>();
            for (Request queuedRequest : transaction.get()) {
                responses.add(executeCommand(server, queuedRequest));
            }
            return RedisToken.array(responses);
        } else {
            return RedisToken.error("ERR EXEC without MULTI");
        }
    }

    /**
     * 执行命令
     * @param server
     * @param queuedRequest
     * @return
     */
    private RedisToken executeCommand(DBServerContext server, Request queuedRequest) {
        RespCommand command = server.getCommand(queuedRequest.getCommand());
        return command.execute(queuedRequest);
    }

    /**
     * 获取事务
     * @param session
     * @return
     */
    private Option<TransactionState> getTransactionIfExists(Session session) {
        return session.removeValue("tx");
    }
}
