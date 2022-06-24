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
import org.frameworkset.tran.DefualtExportResultHandler;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.WrapedExportResultHandler;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.BaseConfig;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.util.RecordGenerator;

import java.io.Writer;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/28 16:48
 * @author biaoping.yin
 * @version 1.0
 */
public class DummyOutputConfig extends BaseConfig implements OutputConfig {
	private boolean printRecord;
	/**
	 * 输出文件记录处理器:org.frameworkset.tran.util.ReocordGenerator
	 */
	private RecordGenerator recordGenerator;
	public boolean isPrintRecord() {
		return printRecord;
	}

	public DummyOutputConfig setPrintRecord(boolean printRecord) {
		this.printRecord = printRecord;
		return this;
	}

	public RecordGenerator getRecordGenerator() {
		return recordGenerator;
	}

	public DummyOutputConfig setRecordGenerator(RecordGenerator recordGenerator) {
		this.recordGenerator = recordGenerator;
		return this;
	}

	@Override
	public void build(ImportBuilder importBuilder) {

	}
	public void generateReocord(org.frameworkset.tran.context.Context taskContext, CommonRecord record, Writer builder)  throws Exception{
		if(builder == null){
			builder = RecordGenerator.tranDummyWriter;
		}
		getRecordGenerator().buildRecord(  taskContext, record,  builder);
	}
	@Override
	public OutputPlugin getOutputPlugin(ImportContext importContext) {
		return new DummyOutputDataTranPlugin(importContext);
	}

	@Override
	public WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler) {
		DefualtExportResultHandler db2ESExportResultHandler = new DefualtExportResultHandler(exportResultHandler);
		return db2ESExportResultHandler;
	}
}
