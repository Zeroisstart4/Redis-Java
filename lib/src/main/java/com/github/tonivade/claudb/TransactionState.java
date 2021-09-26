/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.github.tonivade.resp.command.Request;

// 事务状态
public class TransactionState implements Iterable<Request> {

  // 请求列表
  private final List<Request> requests = new LinkedList<>();

  // 请求入队
  public void enqueue(Request request) {
    requests.add(request);
  }

  // 请求个数
  public int size() {
    return requests.size();
  }

  // 遍历
  @Override
  public Iterator<Request> iterator() {
    return requests.iterator();
  }
}
