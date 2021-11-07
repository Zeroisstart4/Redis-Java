/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.bitset;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.command.annotation.ParamType;
import com.github.tonivade.claudb.data.DataType;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.claudb.data.DatabaseValue;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;

import java.util.BitSet;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.claudb.data.DatabaseValue.bitset;
import static com.github.tonivade.resp.protocol.RedisToken.error;
import static com.github.tonivade.resp.protocol.RedisToken.integer;

/**
 * @author zhou <br/>
 * <p>
 * redis Bitmaps getbit 命令实现。
 */
@Command("getbit")
@ParamLength(2)
@ParamType(DataType.STRING)
public class GetBitCommand implements DBCommand {

    /**
     * 命令形式： getbit key offset 获取指定key对应偏移量上的bit值
     *
     * @param db      当前数据库
     * @param request 命令请求
     * @return
     */
    @Override
    public RedisToken execute(Database db, Request request) {
        try {
            // 获取偏移量
            int offset = Integer.parseInt(request.getParam(1).toString());
            // 获取请求参数 key
            DatabaseValue value = db.getOrDefault(safeKey(request.getParam(0)), bitset());
            // 将该参数转为位集
            BitSet bitSet = BitSet.valueOf(value.getString().getBuffer());
            // 获取指定key对应偏移量上的bit值
            return integer(bitSet.get(offset));
        } catch (NumberFormatException e) {
            return error("bit offset is not an integer");
        }
    }
}
