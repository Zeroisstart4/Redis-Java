/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.persistence;

import java.io.InputStream;
import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

/**
 * @author zhou <br/>
 * <p>
 * 字节缓冲区输入流
 */
public class ByteBufferInputStream extends InputStream {

    /**
     * 字节缓冲区
      */
    private final ByteBuffer buffer;

    public ByteBufferInputStream(byte[] array) {
        this.buffer = ByteBuffer.wrap(requireNonNull(array));
    }

    @Override
    public int read() {
        if (!buffer.hasRemaining()) {
            return -1;
        }
        return buffer.get() & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) {
        int available = Math.min(len, buffer.remaining());
        buffer.get(b, off, available);
        return available;
    }

    @Override
    public int available() {
        return buffer.remaining();
    }
}
