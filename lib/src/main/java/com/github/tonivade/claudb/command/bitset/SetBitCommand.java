/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.bitset;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.command.annotation.ParamType;
import com.github.tonivade.claudb.data.DataType;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;

import java.util.BitSet;
import java.util.LinkedList;
import java.util.Queue;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.claudb.data.DatabaseValue.bitset;
import static com.github.tonivade.resp.protocol.RedisToken.error;
import static com.github.tonivade.resp.protocol.RedisToken.integer;

/**
 * @author zhou <br/>
 * <p>
 * redis Bitmaps setbit 命令实现。
 */
@Command("setbit")
@ParamLength(3)
@ParamType(DataType.STRING)
public class SetBitCommand implements DBCommand {

    /**
     * 命令形式： setbit key offset value 设置或者清空 key 的 value (字符串)在 offset 处的 bit 值。
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
            // 获取位值
            int bit = Integer.parseInt(request.getParam(2).toString());
            Queue<Boolean> queue = new LinkedList<>();
            // 数据合并
            db.merge(safeKey(request.getParam(0)), bitset(), (oldValue, newValue) -> {
                BitSet bitSet = BitSet.valueOf(oldValue.getString().getBuffer());
                queue.add(bitSet.get(offset));
                bitSet.set(offset, bit != 0);
                return oldValue;
            });
            return integer(queue.poll());
        } catch (NumberFormatException e) {
            return error("bit or offset is not an integer");
        }
    }
}
