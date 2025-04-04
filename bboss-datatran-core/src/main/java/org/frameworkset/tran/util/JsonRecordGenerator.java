package org.frameworkset.tran.util;
/**
 * Copyright 2020 bboss
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

import org.frameworkset.elasticsearch.serial.SerialUtil;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.schedule.TaskContext;

import java.io.Writer;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/28 16:52
 * @author biaoping.yin
 * @version 1.0
 */
public class JsonRecordGenerator implements RecordGeneratorV1 {
//	public void buildRecord(TaskContext context, TaskMetrics taskMetrics, CommonRecord record, Writer builder){
    public void buildRecord(RecordGeneratorContext recordGeneratorContext){
        CommonRecord record = recordGeneratorContext.getRecord();
        Writer builder = recordGeneratorContext.getBuilder();
		if(builder != null)
			SerialUtil.object2jsonDisableCloseAndFlush(record.getDatas(),builder);
	}
}
