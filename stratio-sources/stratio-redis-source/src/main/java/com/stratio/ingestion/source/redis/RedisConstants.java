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
package com.stratio.ingestion.source.redis;

public class RedisConstants {
	public static final String CONF_HOST = "host";
    public static final String CONF_PORT = "port";
    public static final String CONF_CHANNELS = "channels";
    public static final String CONF_CHARSET = "charset";


    public static final String DEFAULT_HOST = "localhost";
    public static final Integer DEFAULT_PORT = 6379;
    public static final String DEFAULT_CHARSET = "utf-8";

}
