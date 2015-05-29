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
package com.stratio.ingestion.detector.source.http.handler;

/**
 * Created by mariomgal on 27/5/15.
 */
public class PathId {

    private String asset;
    private Long pathId = 0L;

    public PathId(String asset, Long pathId) {
        this.asset = asset;
        this.pathId = pathId;
    }

    public String getPathId() {
        return asset + "-" + pathId;
    }

    public String getPreviousPathId() {
        return asset + "-" + (pathId-1L);
    }

    public PathId next() {
        return new PathId(asset, pathId + 1L);
    }

    public String toString() {
        return asset + "-" + pathId;
    }
}
