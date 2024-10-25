package org.frameworkset.tran.plugin.custom.output;
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

import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.List;

/**
 * <p>Description: 封装需要处理的数据和其他作业上下文信息</p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/10/24
 */
public class CustomOutPutContext {
    private TaskContext taskContext;
    private TaskMetrics taskMetrics;
    private List<CommonRecord> datas;

    public TaskContext getTaskContext() {
        return taskContext;
    }

    public CustomOutPutContext setTaskContext(TaskContext taskContext) {
        this.taskContext = taskContext;
        return this;
    }

    public TaskMetrics getTaskMetrics() {
        return taskMetrics;
    }

    public CustomOutPutContext setTaskMetrics(TaskMetrics taskMetrics) {
        this.taskMetrics = taskMetrics;
        return this;
    }

    public List<CommonRecord> getDatas() {
        return datas;
    }

    public CustomOutPutContext setDatas(List<CommonRecord> datas) {
        this.datas = datas;
        return this;
    }
}
