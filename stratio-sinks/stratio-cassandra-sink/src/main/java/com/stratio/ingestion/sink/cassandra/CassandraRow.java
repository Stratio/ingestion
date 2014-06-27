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

import java.util.List;

class CassandraRow {

    @SuppressWarnings("rawtypes")
    private final List<CassandraField<?>> fields;

    @SuppressWarnings("rawtypes")
    public CassandraRow(List<CassandraField<?>> fields) {
        this.fields = fields;
    }

    @SuppressWarnings("rawtypes")
    public List<CassandraField<?>> getFields() {
        return this.fields;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.fields == null ? 0 : this.fields.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        CassandraRow other = (CassandraRow) obj;
        if (this.fields == null) {
            if (other.fields != null) {
                return false;
            }
        } else if (!this.fields.equals(other.fields)) {
            return false;
        }
        return true;
    }
}
