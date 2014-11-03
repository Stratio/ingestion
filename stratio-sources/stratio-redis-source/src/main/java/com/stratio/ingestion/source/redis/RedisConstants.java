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
    public static final String CONF_CHANNELS = "subscribe";
    public static final String CONF_PCHANNELS = "psubscribe";
    public static final String CONF_CHARSET = "charset";

    public static final String CONF_TESTONBORROW = "testOnBorrow";
    public static final String CONF_MAXTOTAL = "maxTotal";
    public static final String CONF_MAXIDLE= "maxIdle";
    public static final String CONF_MINIDLE = "minIdle";
    public static final String CONF_MAXWAITINMILLIS = "maxWaitInMillis";
    public static final String CONF_TESTWHILEIDLE = "testWhileIdle";
    public static final String CONF_TESTONRETURN = "testOnReturn";
    public static final String CONF_MINEVICTABLEIDLETIMEINMILLIS = "minEvictableIdleTimeInMillis";
    public static final String CONF_TIMEBETWEETNEVICTIONRUNSMILLIS = "timeBetweenEvictionRunsMillis";
    public static final String CONF_NUMTESTSPEREVICTIONRUN = "numTestsPerEvictionRun";


    public static final String DEFAULT_HOST = "localhost";
    public static final Integer DEFAULT_PORT = 6379;
    public static final String DEFAULT_CHARSET = "utf-8";


    public static final Boolean DEFAULT_TESTONBORROW = true;
    public static final Integer DEFAULT_MAXTOTAL = 100;
    public static final Integer DEFAULT_MAXIDLE = 10;
    public static final Integer DEFAULT_MINIDLE = 2;
    public static final Integer DEFAULT_MAXWAITINMILLIS = 100;
    public static final Boolean DEFAULT_TESTWHILEIDLE = true;
    public static final Boolean DEFAULT_TESTONRETURN = true;
    public static final Integer DEFAULT_MINEVICTABLEIDLETIMEINMILLIS = 10000;
    public static final Integer DEFAULT_TIMEBETWEETNEVICTIONRUNSMILLIS = 5000;
    public static final Integer DEFAULT_NUMTESTSPEREVICTIONRUN = 10;

}
