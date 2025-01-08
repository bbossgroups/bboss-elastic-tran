package org.frameworkset.tran.plugin.multi.output;
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
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.plugin.custom.output.CustomOutPutContext;
import org.frameworkset.tran.plugin.custom.output.CustomOutputConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.task.TaskCommandContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/3/1 11:32
 * @author biaoping.yin
 * @version 1.0
 */
public class MultiTaskCommandImpl extends BaseTaskCommand< String> {
	private static final Logger logger = LoggerFactory.getLogger(MultiTaskCommandImpl.class);
	
	private TaskContext taskContext;
	private MultiOutputConfig multiOutputConfig;
    private MultiOutputDataTranPlugin multiOutputDataTranPlugin ;
    private InnerCommandBuilder innerCommandBuilder;
	public MultiTaskCommandImpl(TaskCommandContext taskCommandContext, InnerCommandBuilder innerCommandBuilder, OutputConfig outputConfig) {
		super(  outputConfig,taskCommandContext);
        multiOutputConfig = (MultiOutputConfig) outputConfig;
        multiOutputDataTranPlugin = (MultiOutputDataTranPlugin) outputConfig.getOutputPlugin();
		if(this.taskContext == null) {
            this.taskContext = new TaskContext(importContext);
            taskCommandContext.setTaskContext(taskContext);
        }
        this.innerCommandBuilder = innerCommandBuilder;
	}

	public String execute(){
        if(records != null && records.size() > 0) {
            ExecutorService executorService = multiOutputDataTranPlugin.buildmultiOutputExecutor();
            List<Future> futures = this.innerCommandBuilder.buildBaseTaskCommand(records,   executorService);
            List<Throwable> exceptions = new ArrayList<>();
            for(Future future:futures){
                try {
                    future.get();
                } catch (InterruptedException e) {
                     
                } catch (ExecutionException e) {
                    exceptions.add( e.getCause());
                }
            }

            if(exceptions.size() > 0)
                throw ImportExceptionUtil.buildDataImportException(importContext,"MultiTaskCommandImpl exceptions occur",exceptions);
            
        }        
		finishTask();
		return null;
	}



}
