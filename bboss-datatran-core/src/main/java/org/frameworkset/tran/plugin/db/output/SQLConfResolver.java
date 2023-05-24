package org.frameworkset.tran.plugin.db.output;
/**
 * Copyright 2023 bboss
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

import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.schedule.TaskContext;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/5/24
 * @author biaoping.yin
 * @version 1.0
 */
public interface SQLConfResolver {
    /**
     * 根据记录信息识别对应的SQLConf对应的key（通常代表表名称）
     * mysql binlog输入插件对接时，默认使用表名称映射对应的sqlconf配置
     * 其他场景需要通过SQLConfResolver接口从当前记录中获取对应的字段值作为sqlconf配置对应的映射名称
     * @param taskContext
     * @param record
     * @return
     */
    public String resolver(TaskContext taskContext, CommonRecord record);
}
