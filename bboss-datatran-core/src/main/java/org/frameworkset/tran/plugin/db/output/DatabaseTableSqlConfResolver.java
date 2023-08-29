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
 * <p>Description: 根据记录中的数据库和表元数据信息，获取记录对应的数据库和表组合配置key名称,格式为：database.tablename</p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/8/29
 * @author biaoping.yin
 * @version 1.0
 */
public class DatabaseTableSqlConfResolver implements SQLConfResolver{
    @Override
    public String resolver(TaskContext taskContext, CommonRecord record) {
        StringBuilder ret = new StringBuilder();
        ret.append(record.getMetaValue("database")).append(".").append(record.getMetaValue("table"));
        return ret.toString();
    }
}
