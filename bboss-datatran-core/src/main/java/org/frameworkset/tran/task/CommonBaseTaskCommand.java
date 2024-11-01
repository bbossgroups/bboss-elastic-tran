package org.frameworkset.tran.task;
/**
 * Copyright 2008 biaoping.yin
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
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.context.JobContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/11/4 16:50
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class CommonBaseTaskCommand<Object> extends BaseTaskCommand<Object> {

    private static Logger logger = LoggerFactory.getLogger(CommonBaseTaskCommand.class);

    protected String taskInfo;

    private int tryCount;
    public CommonBaseTaskCommand(TaskCommandContext taskCommandContext){
        super(taskCommandContext);
        this.taskInfo = taskCommandContext.getTaskInfo();
    }
    public int getTryCount() {
        return tryCount;
    }
    protected abstract Object _execute(  );
    @Override
    public Object execute(){
        Object data = null;
        if(records.size() > 0) {

            if(this.importContext.getMaxRetry() > 0){
                if(this.tryCount >= this.importContext.getMaxRetry())
                    throw new TaskFailedException("task execute failed:reached max retry times "+this.importContext.getMaxRetry());
            }
            this.tryCount ++;

            try {

                Object bulkWriteResult =  _execute(  );
                if(bulkWriteResult != null){
                    data = bulkWriteResult;

                }

            }

            catch (Exception e) {
                throw ImportExceptionUtil.buildDataImportException(importContext,taskInfo,e);
            }

            catch (Throwable e) {
                throw ImportExceptionUtil.buildDataImportException(importContext,taskInfo,e);
            }
        }
        else{
            logNodatas( logger);
        }
        finishTask();
        return data;
    }

 
}
