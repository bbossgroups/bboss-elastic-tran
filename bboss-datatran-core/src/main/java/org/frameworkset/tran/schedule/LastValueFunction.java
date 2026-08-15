package org.frameworkset.tran.schedule;
/**
 * Copyright 2026 bboss
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.frameworkset.tran.Record;

/**
 * 获取增量值函数接口：实现增量同步函数方式获取增量字段值功能，用于处理在复杂结构中获取增量字段值，同时务必设置增量字段名称
 * @author biaoping.yin
 * @Date 2026/8/14
 */
public interface LastValueFunction<T> {
	T getLastValue(Record record,String colName); 
}
