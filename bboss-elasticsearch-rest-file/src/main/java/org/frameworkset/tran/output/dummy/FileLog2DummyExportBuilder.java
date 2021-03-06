package org.frameworkset.tran.output.dummy;
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.input.file.FileBaseDataTranPlugin;
import org.frameworkset.tran.ouput.dummy.DummyOupputConfig;
import org.frameworkset.tran.ouput.dummy.DummyOupputContextImpl;
import org.frameworkset.tran.output.FileLogBaseImportBuilder;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/1/11 21:29
 * @author biaoping.yin
 * @version 1.0
 */
public class FileLog2DummyExportBuilder extends FileLogBaseImportBuilder {


	@JsonIgnore
	private DummyOupputConfig dummyOupputConfig;






	protected ImportContext buildTargetImportContext(BaseImportConfig importConfig){
		DummyOupputContextImpl dummyOupputContext = new DummyOupputContextImpl((DummyOupputConfig) importConfig);
		dummyOupputContext.init();
		return dummyOupputContext;
	}


	protected void setTargetImportContext(DataStream dataStream){
		if(dummyOupputConfig != null)
			dataStream.setTargetImportContext(buildTargetImportContext(dummyOupputConfig) );
		else
			throw new DataImportException("DummyOupputConfig is null,please set it as:\n\t\tDummyOupputConfig dummyOupputConfig = new DummyOupputConfig();\n" +
					"\t\tdummyOupputConfig.setRecordGenerator(new RecordGenerator() {\n" +
					"\t\t\t@Override\n" +
					"\t\t\tpublic void buildRecord(Context taskContext, CommonRecord record, Writer builder) throws Exception{\n" +
					"\t\t\t\tSimpleStringUtil.object2json(record.getDatas(),builder);\n" +
					"\n" +
					"\t\t\t}\n" +
					"\t\t}).setPrintRecord(true);\n" +
					"\t\timportBuilder.setDummyOupputConfig(dummyOupputConfig);");
	}

	@Override
	protected FileBaseDataTranPlugin createFileBaseDataTranPlugin(ImportContext importContext, ImportContext targetImportContext) {
		return new FileLog2DummyDataTranPlugin(importContext,targetImportContext);
	}

	public DummyOupputConfig getDummyOupputConfig() {
		return dummyOupputConfig;
	}

	public FileLog2DummyExportBuilder setDummyOupputConfig(DummyOupputConfig dummyOupputConfig) {
		this.dummyOupputConfig = dummyOupputConfig;
		return this;
	}

}
