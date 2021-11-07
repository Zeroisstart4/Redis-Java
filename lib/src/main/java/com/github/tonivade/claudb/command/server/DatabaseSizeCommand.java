package com.github.tonivade.claudb.command.server;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;

import static com.github.tonivade.resp.protocol.RedisToken.integer;

/**
 * @author zhou <br/>
 * <p>
 * redis 数据库相关命令的 dbsize 命令实现。
 */
@Command("dbsize")
public class DatabaseSizeCommand implements DBCommand {

    /**
     * 命令形式： dbsize 返回当前数据里面 keys 的数量。
     * @param db      当前数据库
     * @param request 命令请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        return integer(db.size());
    }
}
