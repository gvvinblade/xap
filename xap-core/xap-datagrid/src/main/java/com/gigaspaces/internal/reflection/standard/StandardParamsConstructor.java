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

package com.gigaspaces.internal.reflection.standard;

import com.gigaspaces.internal.reflection.IParamsConstructor;
import com.gigaspaces.metadata.SpaceMetadataException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Provides a wrapper over the standard parameter constructor reflection
 *
 * @author Dan Kilman
 * @since 9.6
 */
@com.gigaspaces.api.InternalApi
public class StandardParamsConstructor<T> implements IParamsConstructor<T> {

    private static final long serialVersionUID = 1L;

    final private Constructor<T> ctor;

    public StandardParamsConstructor(Constructor<T> ctor) {
        ctor.setAccessible(true);
        this.ctor = ctor;
    }

    public T newInstance(Object... params) {
        try {
            return ctor.newInstance(params);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new SpaceMetadataException("Failed to create parameterized new instance of " + ctor.getDeclaringClass(), e);
        }
    }
}
