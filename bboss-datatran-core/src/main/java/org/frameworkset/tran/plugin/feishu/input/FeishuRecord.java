package org.frameworkset.tran.plugin.feishu.input;
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

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.record.CommonMapRecord;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author biaoping.yin
 * @Date 2026/3/23
 */
public class FeishuRecord extends CommonMapRecord {
    private String recordId;
    public FeishuRecord(  Map record, TaskContext taskContext, ImportContext importContext) {
        super(taskContext,  importContext,record);
         
    }

    public void initMetaDatas() {
        Map<String, Object> tmp = new LinkedHashMap<>();
        tmp.put("recordId", this.getRecordId());       
        this.setMetaDatas(tmp);
    }

    public String getRecordId() {
        return recordId;
    }

    public FeishuRecord setRecordId(String recordId) {
        this.recordId = recordId;
        return this;
    }
}
