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
package com.stratio.ingestion.morphline.checkpointfilter.builder;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.stratio.ingestion.morphline.checkpointfilter.handler.CheckpointFilterHandler;
import com.stratio.ingestion.morphline.checkpointfilter.type.CheckpointType;

/**
 * Created by eambrosio on 14/01/15.
 */
public abstract class AbstractCheckpointFilter {

    private static Class[] parameterClasses = new Class[] { CheckpointType.class, Map.class };

    public static CheckpointType getTypeInstance(Map<String, String> context) {
        return getInstance(context.get("type"), CheckpointType.class, null, null);
    }

    public static CheckpointFilterHandler getHandlerInstance(CheckpointType type,
            Map<String, String> context) {
        final Object[] parameters = buildParametersArray(type, context);
        return getInstance(context.get("handler"), CheckpointFilterHandler.class, parameterClasses, parameters);
    }

    private static Object[] buildParametersArray(CheckpointType type, Map<String, String> context) {
        List<Object> parameters = new ArrayList<Object>(0);
        parameters.add(type);
        parameters.add(new HashMap<String, String>(context));
        return parameters.toArray();
    }

    private static <T> T getInstance(String className, Class<T> clazz, Class[] classes, Object... parameters) {
        T instance = null;
        try {
            if (parameters != null) {
                instance = clazz
                        .cast(Class.forName(className).getDeclaredConstructor(classes).newInstance(parameters));
            } else {
                instance = clazz
                        .cast(Class.forName(className).newInstance());
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

    private static Class<?>[] getClasses(Object[] parameters) throws ClassNotFoundException {
        //        List<Class<?>> classes = new ArrayList<Class<?>>();
        Class<?>[] array = new Class[parameters.length];
        for (int i = 0; i < parameters.length; i++) {
            array[i] = parameters[i].getClass();
            //            Object parameter = parameters[i];
            //            classes.add(Class.forName(parameter.getClass().getName()));

        }
        return array;
    }
}
