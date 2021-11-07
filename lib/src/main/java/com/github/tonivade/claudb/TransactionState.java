/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb;

import com.github.tonivade.resp.command.Request;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author zhou <br/>
 * <p>
 * 事务状态类
 */
public class TransactionState implements Iterable<Request> {

    /**
     * 请求列表
     */
    private final List<Request> requests = new LinkedList<>();

    /**
     * 请求入队
     *
     * @param request
     */
    public void enqueue(Request request) {
        requests.add(request);
    }

    /**
     * 请求个数
     *
     * @return
     */
    public int size() {
        return requests.size();
    }

    /**
     * 请求列表迭代器
     *
     * @return
     */
    @Override
    public Iterator<Request> iterator() {
        return requests.iterator();
    }
}
