package org.frameworkset.tran.plugin.dummy.output;
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

import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.plugin.BaseConfig;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.util.*;

import java.io.Writer;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/28 16:48
 * @author biaoping.yin
 * @version 1.0
 */
public class DummyOutputConfig extends BaseConfig<DummyOutputConfig> implements OutputConfig<DummyOutputConfig> {
	private boolean printRecord;
	/**
	 * 输出文件记录处理器:org.frameworkset.tran.util.ReocordGenerator
	 */
    @Deprecated
	private RecordGenerator recordGenerator;
    private RecordGeneratorV1 recordGeneratorV1;
	public boolean isPrintRecord() {
		return printRecord;
	}

	public DummyOutputConfig setPrintRecord(boolean printRecord) {
		this.printRecord = printRecord;
		return this;
	}

    @Deprecated
	public DummyOutputConfig setRecordGenerator(RecordGenerator recordGenerator) {
		this.recordGenerator = recordGenerator;
		return this;
	}

	@Override
	public void build(ImportContext importContext,ImportBuilder importBuilder) {
		if(recordGenerator == null && recordGeneratorV1 == null){
            recordGeneratorV1 = new JsonRecordGenerator();
		}
        if(recordGeneratorV1 == null){
            recordGeneratorV1 = new DefaultRecordGeneratorV1(recordGenerator);
        }
	}
	public void generateReocord(TaskContext taskContext, TaskMetrics taskMetrics, CommonRecord record, Writer builder)  throws Exception{
		if(builder == null){
			builder = RecordGeneratorV1.tranDummyWriter;
		}
        RecordGeneratorContext recordGeneratorContext = new RecordGeneratorContext();
        recordGeneratorContext.setRecord(record)
                .setTaskContext(taskContext)
                .setBuilder(builder)
                .setTaskMetrics(taskMetrics).setMetricsLogAPI(taskContext.getDataTranPlugin());       
        
		getRecordGeneratorV1().buildRecord(  recordGeneratorContext);
	}
	@Override
	public OutputPlugin getOutputPlugin(ImportContext importContext) {
		return new DummyOutputDataTranPlugin(this,importContext);
	}

    public RecordGeneratorV1 getRecordGeneratorV1() {
        return recordGeneratorV1;
    }

    public DummyOutputConfig setRecordGeneratorV1(RecordGeneratorV1 recordGeneratorV1) {
        this.recordGeneratorV1 = recordGeneratorV1;
        return this;
    }
}
