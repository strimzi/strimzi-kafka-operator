/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.enmasse.barnabas.operator.topic;

public class AsyncResult<T> {

    private final T result;
    private final Throwable error;

    private AsyncResult(T result, Throwable error) {
        this.result = result;
        this.error = error;
    }

    public static <T> AsyncResult<T> success(T result) {
        return new AsyncResult(result, null);
    }

    public static AsyncResult failure(Throwable error) {
        if (error == null) {
            throw new NullPointerException();
        }
        return new AsyncResult(null, error);
    }

    public T result() {
        if (error != null) {
            throw new RuntimeException(error);
        }
        return result;
    }

    public boolean isSuccess() {
        return error == null;
    }

    public Throwable exception() {
        return error;
    }
}
