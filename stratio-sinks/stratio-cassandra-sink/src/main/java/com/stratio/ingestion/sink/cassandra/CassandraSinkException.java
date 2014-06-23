/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.ingestion.sink.cassandra;

class CassandraSinkException extends RuntimeException {

    private static final long serialVersionUID = -5451808239155117379L;

    public CassandraSinkException(String message) {
        super(message);
    }

    public CassandraSinkException(Throwable cause) {
        super(cause);
    }

    public CassandraSinkException(String message, Throwable cause) {
        super(message, cause);
    }
}
