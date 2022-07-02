package org.frameworkset.tran.plugin.es.input;
/**
 * Copyright 2020 bboss
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

import org.frameworkset.tran.schedule.TaskContext;

import java.util.Date;

/**
 * <p>Description: 动态生成查询的请求地址</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/2/2 15:21
 * @author biaoping.yin
 * @version 1.0
 */
public interface QueryUrlFunction {
	/**
	 * 根据数据检索起始时间获取elasticsearch检索数据索引名称范围，适用于按照时间维度分表的场景
	 * @param lastStartValue
	 * @param lastEndValue
	 * @return
	 */
	public String queryUrl(TaskContext taskContext,Date lastStartValue, Date lastEndValue);
}
