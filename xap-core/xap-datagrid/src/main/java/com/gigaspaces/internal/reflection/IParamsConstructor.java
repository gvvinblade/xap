/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gigaspaces.internal.reflection;

import java.io.Serializable;

/**
 * Provides an abstraction over a constructor with parameters reflection.
 *
 * @param <T> Type of the class that contains this constructor.
 * @author Dan Kilman
 * @since 9.6
 */
public interface IParamsConstructor<T> extends Serializable {

    T newInstance(Object... args);
}
