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
package com.stratio.ingestion.source.rest.url.filter;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.stratio.ingestion.source.rest.url.filter.type.CheckpointType;

/**
 * Created by eambrosio on 14/01/15.
 */
public abstract class FilterHandler {

    private static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX";
    String filterField;
    CheckpointType filterType;
    Map<String, String> context;

    /**
     * Obtain the last checkpoint element
     *
     * @param context Context properties map
     * @return Checkpoint value
     */
    public abstract Map<String,String> getLastFilter(Map<String, String> context);

    /**
     * Update the checkpoint value
     *
     * @param checkpoint checkpoint value
     */
    public abstract void updateFilter(String checkpoint);

    public abstract void configure(Map<String, String> context);

    public static <T> T getInstance(String fieldName, Class<T> type, Object... parameters) {
        T instance = null;
        try {
            if (parameters != null) {

                instance = type
                        .cast(Class.forName(fieldName).newInstance());
            } else {
                instance = type
                        .cast(Class.forName(fieldName).getDeclaredConstructor(getClasses(parameters)).newInstance
                                (parameters));
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return instance;
    }

    private static Class<?>[] getClasses(Object[] parameters) {
        List<Class<?>> classes = new ArrayList<Class<?>>();

        for (int i = 0; i < parameters.length; i++) {
            Object parameter = parameters[i];
            classes.add(parameter.getClass());

        }
        return (Class<?>[]) classes.toArray();
    }
}
