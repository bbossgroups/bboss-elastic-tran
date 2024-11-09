package org.frameworkset.tran.plugin.metrics.output;
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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.metrics.job.Metrics;
import org.frameworkset.tran.metrics.job.MetricsLogAPI;
import org.frameworkset.tran.schedule.TaskContext;

/**
 * <p>Description: etl指标计算器</p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/2/13
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class ETLMetrics extends Metrics {

    protected ImportContext importContext;

    private String dataTimeField;

    private SimpleBuildMapData buildMapData;
    public ETLMetrics() {
        super();
    }
    @Override
    protected MetricsLogAPI buildMetricsLogAPI(){
        return new MetricsLogAPI<TaskContext, TaskMetrics>() {
            /**
             * 记录作业处理过程中的异常日志
             *
             * @param logcontext
             * @param msg
             * @param e
             */
            @Override
            public void reportJobMetricErrorLog(TaskContext logcontext, String msg, Throwable e) {
                importContext.reportJobMetricErrorLog(logcontext, msg, e);
            }

            /**
             * 记录作业处理过程中的异常日志
             *
             * @param msg
             * @param e
             */
            @Override
            public void reportJobMetricErrorLog(String msg, Throwable e) {
                importContext.reportJobMetricErrorLog(msg, e);
            }

            /**
             * 记录作业处理过程中的日志
             *
             * @param msg
             */
            @Override
            public void reportJobMetricLog(String msg) {
                importContext.reportJobMetricLog(msg);
            }

            /**
             * 记录作业处理过程中的告警日志
             *
             * @param msg
             */
            @Override
            public void reportJobMetricWarn(String msg) {
                importContext.reportJobMetricWarn(msg);
            }

            /**
             * 记录作业处理过程中的debug日志
             *
             * @param msg
             */
            @Override
            public void reportJobMetricDebug(String msg) {
                importContext.reportJobMetricDebug(msg);
            }

            /**
             * 记录作业处理过程中的日志
             *
             * @param logcontext
             * @param msg
             */
            @Override
            public void reportJobMetricLog(TaskContext logcontext, String msg) {
                importContext.reportJobMetricLog(logcontext, msg);
            }

            /**
             * 记录作业处理过程中的告警日志
             *
             * @param logcontext
             * @param msg
             */
            @Override
            public void reportJobMetricWarn(TaskContext logcontext, String msg) {
                importContext.reportJobMetricWarn(logcontext, msg);
            }

            /**
             * 记录作业处理过程中的日志
             *
             * @param logcontext
             * @param msg
             */
            @Override
            public void reportJobMetricDebug(TaskContext logcontext, String msg) {
                importContext.reportJobMetricDebug(logcontext, msg);
            }

            /**
             * 记录作业任务处理过程中的异常日志
             *
             * @param logcontext
             * @param msg
             * @param e
             */
            @Override
            public void reportTaskMetricErrorLog(TaskMetrics logcontext, String msg, Throwable e) {
                importContext.reportTaskMetricErrorLog(logcontext, msg, e);
            }

            /**
             * 记录作业任务处理过程中的日志
             *
             * @param logcontext
             * @param msg
             */
            @Override
            public void reportTaskMetricLog(TaskMetrics logcontext, String msg) {
                importContext.reportTaskMetricLog(logcontext, msg);
            }

            /**
             * 记录作业任务处理过程中的告警日志
             *
             * @param logcontext
             * @param msg
             */
            @Override
            public void reportTaskMetricWarn(TaskMetrics logcontext, String msg) {
                if(logcontext != null && logcontext instanceof TaskMetrics)
                importContext.reportTaskMetricWarn(logcontext, msg);
            }

            /**
             * 记录作业任务处理过程中的debug日志
             *
             * @param logcontext
             * @param msg
             */
            @Override
            public void reportTaskMetricDebug(TaskMetrics logcontext, String msg) {
                if(logcontext != null && logcontext instanceof TaskMetrics)
                    importContext.reportTaskMetricDebug(logcontext, msg);
            }
        };
    }
    public ETLMetrics(int metricsType) {
        super(metricsType);
    }
    public ETLMetrics setBuildMapData(SimpleBuildMapData buildMapData) {
        this.buildMapData = buildMapData;
        return this;
    }

    public SimpleBuildMapData getBuildMapData() {
        return buildMapData;
    }

    /**
     * 默认构建MapData数据，如果有需要额外定义MapData，则可以继承并重新实现方法
     * @param metricsData
     * @return
     */
    public ETLMapData buildMapData(MetricsData metricsData){
        ETLMapData etlMapData = null;
        if(buildMapData != null){
            if(buildMapData instanceof BuildMapData) {
                etlMapData = buildMapData.buildMapData(metricsData);
            }
            else {
                etlMapData = buildMapData.buildMapData(metricsData);
                metricsData.setData(etlMapData, this);
            }
        }
        else {
            etlMapData = new ETLMapData();           
            metricsData.setData(etlMapData, this);
        }
        etlMapData.setTaskMetrics(metricsData.getTaskMetrics());
        return etlMapData;

    }
    /**
     * 获取指标时间维度字段，不是设置默认采用当前时间，否则采用字段对应的时间值
     * 可以重新实现方法，返回特定的指标时间维度字段
     * @param metricsData
     */
    public String getDataTimeField(MetricsData metricsData){
        if(SimpleStringUtil.isNotEmpty(dataTimeField ))
            return dataTimeField;
        return metricsData.getBuildMapDataContext().getDataTimeField();
    }
    public String getDataTimeField() {
        return dataTimeField;
    }

    public void setDataTimeField(String dataTimeField) {
        this.dataTimeField = dataTimeField;
    }

    public ImportContext getImportContext() {
        return importContext;
    }

    public void setImportContext(ImportContext importContext) {
        this.importContext = importContext;
    }
}
