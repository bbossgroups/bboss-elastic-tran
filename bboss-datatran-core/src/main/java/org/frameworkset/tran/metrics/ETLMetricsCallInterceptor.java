package org.frameworkset.tran.metrics;
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

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.metrics.output.ETLMetrics;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/2/19
 * @author biaoping.yin
 * @version 1.0
 */
public class ETLMetricsCallInterceptor implements CallInterceptor {
    private static Logger logger = LoggerFactory.getLogger(ETLMetricsCallInterceptor.class);
    private List<ETLMetrics> etlMetrics;
    private ImportContext importContext;
    public ETLMetricsCallInterceptor(List<ETLMetrics> etlMetrics,ImportContext importContext){
        this.etlMetrics = etlMetrics;
        this.importContext = importContext;
    }
    @Override
    public void preCall(TaskContext taskContext) {

    }

    @Override
    public void afterCall(TaskContext taskContext) {
        if(etlMetrics == null || etlMetrics.size() == 0)
            return;
        if(logger.isInfoEnabled())
            logger.info("Flush Metrics On Schedule Task Completed begin:CleanKeysWhenflushMetricsOnScheduleTaskCompleted[{}],WaitCompleteWhenflushMetricsOnScheduleTaskCompleted[{}]",
                importContext.isCleanKeysWhenflushMetricsOnScheduleTaskCompleted(),
                importContext.isWaitCompleteWhenflushMetricsOnScheduleTaskCompleted());
        for(ETLMetrics metrics: etlMetrics){
            metrics.forceFlush(importContext.isCleanKeysWhenflushMetricsOnScheduleTaskCompleted(),
                            importContext.isWaitCompleteWhenflushMetricsOnScheduleTaskCompleted());
        }
        if(logger.isInfoEnabled())
            logger.info("Flush Metrics On Schedule Task Completed finished.",
                importContext.isCleanKeysWhenflushMetricsOnScheduleTaskCompleted(),
                importContext.isWaitCompleteWhenflushMetricsOnScheduleTaskCompleted());
    }

    @Override
    public void throwException(TaskContext taskContext, Throwable e) {

    }
}
