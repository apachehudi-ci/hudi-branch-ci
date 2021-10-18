/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.data;

import org.apache.hudi.common.function.SerializableFunction;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

public abstract class HoodieDataV2<T, O> implements Serializable {

  public abstract Object get();

  public abstract boolean isEmpty();

  public abstract HoodieDataV2<T, O> map(SerializableFunction<T, O> func);

  public abstract HoodieDataV2<T, O> flatMap(SerializableFunction<T, Iterator<O>> func);

  public abstract List<T> collectAsList();
}
