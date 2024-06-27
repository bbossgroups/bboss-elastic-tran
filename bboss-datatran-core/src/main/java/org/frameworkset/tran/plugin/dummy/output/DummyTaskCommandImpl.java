package org.frameworkset.tran.plugin.dummy.output;
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

import org.frameworkset.soa.BBossStringWriter;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.task.TaskCommandContext;
import org.frameworkset.tran.util.TranUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/3/1 11:32
 * @author biaoping.yin
 * @version 1.0
 */
public class DummyTaskCommandImpl extends BaseTaskCommand<String> {
	private Logger logger = LoggerFactory.getLogger(DummyTaskCommandImpl.class);
	private DummyOutputConfig dummyOutputConfig ;
	public DummyTaskCommandImpl(TaskCommandContext taskCommandContext) {
		super(  taskCommandContext);
		dummyOutputConfig = (DummyOutputConfig) importContext.getOutputConfig();
	}

    private String buildDatas() throws Exception {
        StringBuilder builder = new StringBuilder();
        BBossStringWriter writer = new BBossStringWriter(builder);
        for(int i = 0; i < records.size(); i ++){
            dummyOutputConfig.generateReocord(taskContext,records.get(i), writer);
            writer.write(TranUtil.lineSeparator);
        }
        return writer.toString();
    }
    private String datas;
    public Object getDatas(){
        return datas;
    }
	public String execute() throws Exception {
        if(records.size() > 0) {
            datas = buildDatas();
            if (dummyOutputConfig.isPrintRecord()) {

                logger.info(datas);
            }
        }
        else{
            logNodatas( logger);
        }
		finishTask();
		return null;
	}

	 


}
