package org.frameworkset.tran.plugin.http;
/**
 * Copyright 2022 bboss
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
import org.frameworkset.tran.metrics.DataTranPluginMetricsLogAPI;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.metrics.job.MetricsLogAPI;
import org.frameworkset.tran.schedule.TaskContext;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/7/24
 * @author biaoping.yin
 * @version 1.0
 */
public class DynamicHeaderContext implements DataTranPluginMetricsLogAPI {
	private ImportContext importContext;
	private TaskContext taskContext;

   

    private TaskMetrics taskMetrics;
	private Object datas;

	public ImportContext getImportContext() {
		return importContext;
	}

	public void setImportContext(ImportContext importContext) {
		this.importContext = importContext;
	}

	public TaskContext getTaskContext() {
		return taskContext;
	}

	public void setTaskContext(TaskContext taskContext) {
		this.taskContext = taskContext;
	}

	public Object getDatas() {
		return datas;
	}

	public void setDatas(Object datas) {
		this.datas = datas;
	}

    /**
     * 记录作业处理过程中的异常日志
     *
     * @param msg
     * @param e
     */
    @Override
    public void reportJobMetricErrorLog(String msg, Throwable e) {
        taskContext.reportJobMetricErrorLog(msg, e);
    }

    /**
     * 记录作业处理过程中的告警日志
     *
     * @param msg
     */
    @Override
    public void reportJobMetricLog(String msg) {
        taskContext.reportJobMetricLog(msg);
    }

    /**
     * 记录作业处理过程中的debug日志
     *
     * @param msg
     */
    @Override
    public void reportJobMetricDebug(String msg) {
        taskContext.reportJobMetricDebug(msg);
    }

    /**
     * 记录作业处理过程中的告警日志
     *
     * @param msg
     */
    @Override
    public void reportJobMetricWarn(String msg) {
        taskContext.reportJobMetricWarn(msg);
    }

    /**
     * 记录作业任务处理过程中的异常日志
     *
     * @param msg
     * @param e
     */
    @Override
    public void reportTaskMetricErrorLog( String msg, Throwable e) {
        taskContext.reportTaskMetricErrorLog(taskMetrics, msg, e);
    }

    /**
     * 记录作业任务处理过程中的日志
     *
     * @param msg
     */
    @Override
    public void reportTaskMetricLog(  String msg) {
        taskContext.reportTaskMetricLog(taskMetrics, msg);
    }

    /**
     * 记录作业任务处理过程中的告警日志
     *
     * @param msg
     */
    @Override
    public void reportTaskMetricWarn( String msg) {
        taskContext.reportTaskMetricWarn(taskMetrics, msg);
    }

    /**
     * 记录作业任务处理过程中的debug日志
     *
     * @param msg
     */
    @Override
    public void reportTaskMetricDebug( String msg) {
        taskContext.reportTaskMetricDebug(taskMetrics, msg);
    }

    public TaskMetrics getTaskMetrics() {
        return taskMetrics;
    }

    public DynamicHeaderContext setTaskMetrics(TaskMetrics taskMetrics) {
        this.taskMetrics = taskMetrics;
        return this;
    }
}
