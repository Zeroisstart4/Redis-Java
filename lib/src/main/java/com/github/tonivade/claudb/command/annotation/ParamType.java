/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.annotation;

import com.github.tonivade.claudb.data.DataType;

import java.lang.annotation.*;

/**
 * @author zhou <br/>
 * <p>
 * 参数类型
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
public @interface ParamType {

    DataType value();
}
