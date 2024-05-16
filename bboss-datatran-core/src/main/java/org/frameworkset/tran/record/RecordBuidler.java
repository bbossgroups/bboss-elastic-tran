package org.frameworkset.tran.record;
/**
 * Copyright 2024 bboss
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

import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.Map;

/**
 * <p>Description: 从原始数据集中构建Record记录对象，通过记录构建器，提前从源数据流中构建一个数据记录，用于后续并发处理</p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/5/16
 */
public interface RecordBuidler<T> {
    Map<String,Object> build(RecordBuidlerContext<T> recordBuidlerContext) throws DataImportException; 
    
}
