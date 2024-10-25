package org.frameworkset.tran.util;
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
import org.frameworkset.tran.metrics.DataTranPluginMetricsLogAPI;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.schedule.TaskContext;

import java.io.Writer;

/**
 * <p>Description: 封装需要处理的数据和其他作业上下文信息</p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/10/24
 */
public class RecordGeneratorContext {
    private TaskContext taskContext; 
    private TaskMetrics taskMetrics;
    private CommonRecord record;
    private Writer builder;
    private DataTranPluginMetricsLogAPI metricsLogAPI;

   

    public TaskContext getTaskContext() {
        return taskContext;
    }

    public RecordGeneratorContext setTaskContext(TaskContext taskContext) {
        this.taskContext = taskContext;
        return this;
    }

    public TaskMetrics getTaskMetrics() {
        return taskMetrics;
    }

    public RecordGeneratorContext setTaskMetrics(TaskMetrics taskMetrics) {
        this.taskMetrics = taskMetrics;
        return this;
    }

    public CommonRecord getRecord() {
        return record;
    }

    public RecordGeneratorContext setRecord(CommonRecord record) {
        this.record = record;
        return this;
    }

    public Writer getBuilder() {
        return builder;
    }

    public RecordGeneratorContext setBuilder(Writer builder) {
        this.builder = builder;
        return this;
    }
    public DataTranPluginMetricsLogAPI getMetricsLogAPI() {
        return metricsLogAPI;
    }

    public RecordGeneratorContext setMetricsLogAPI(DataTranPluginMetricsLogAPI metricsLogAPI) {
        this.metricsLogAPI = metricsLogAPI;
        return this;
    }
}
