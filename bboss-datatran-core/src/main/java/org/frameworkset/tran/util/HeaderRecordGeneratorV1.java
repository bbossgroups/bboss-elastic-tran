package org.frameworkset.tran.util;
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

import java.io.Writer;

/**
 * <p>Description: 用于生成第一行记录，主要用于生成csv文件的title和其他文本的第一行标题</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/28 16:52
 * @author biaoping.yin
 * @version 1.0
 */
public interface HeaderRecordGeneratorV1 extends RecordGeneratorV1{
	/**
	 * 构建头行数据方法
	 * @param recordGeneratorContext
	 * @throws Exception
	 */
	public void buildHeaderRecord(RecordGeneratorContext recordGeneratorContext) throws Exception;
}
