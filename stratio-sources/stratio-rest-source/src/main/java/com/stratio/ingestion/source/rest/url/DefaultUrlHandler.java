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
package com.stratio.ingestion.source.rest.url;

import java.util.Map;

import org.apache.flume.Context;

/**
 * Created by eambrosio on 5/02/15.
 */
public class DefaultUrlHandler implements UrlHandler {
    private static final String URL = "url";

    /**
     * Returns url property value
     *
     * @param properties
     * @return
     * @throws Exception
     */
    @Override public String buildUrl(Map<String, String> properties) {
        return properties.get(URL);
    }

    @Override public void updateFilterParameters(String filterParameters) {

    }

    @Override public void configure(Context context) {

    }
}
