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
package com.stratio.ingestion.source.batch.rest.domain;

import java.util.Date;

/**
 * Created by eambrosio on 30/12/14.
 */

public class Checkpoint {

    private String id;
    private String santanderID;
    private Date date;

    public String getId() {
        return id;
    }

    public String getSantanderID() {
        return santanderID;
    }

    public Date getDate() {
        return date;
    }

    private Checkpoint(Checkpoint.Builder builder) {
        this.id = builder.id;
        this.santanderID = builder.santanderID;
        this.date = builder.date;
    }

    public static class Builder {
        private String id;
        private String santanderID;
        private Date date;

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder santanderID(String santanderID) {
            this.santanderID = santanderID;
            return this;
        }

        public Builder date(Date date) {
            this.date = date;
            return this;
        }

        public Checkpoint build() {
            return new Checkpoint(this);
        }

    }
}
