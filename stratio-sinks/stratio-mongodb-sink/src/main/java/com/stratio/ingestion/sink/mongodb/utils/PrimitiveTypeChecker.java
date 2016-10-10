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
package com.stratio.ingestion.sink.mongodb.utils;

import java.util.HashSet;
import java.util.Set;

public class PrimitiveTypeChecker {

	private static final Set<Class<?>> PRIMITIVE_TYPES = getPrimitiveTypes();

	public static boolean isPrimitiveType(Class<?> clazz) {
		return PRIMITIVE_TYPES.contains(clazz);
	}

	private static Set<Class<?>> getPrimitiveTypes() {
		Set<Class<?>> ret = new HashSet<Class<?>>();
		ret.add(boolean.class);
		ret.add(char.class);
		ret.add(byte.class);
		ret.add(short.class);
		ret.add(int.class);
		ret.add(long.class);
		ret.add(float.class);
		ret.add(double.class);
		ret.add(void.class);
		return ret;
	}

}
