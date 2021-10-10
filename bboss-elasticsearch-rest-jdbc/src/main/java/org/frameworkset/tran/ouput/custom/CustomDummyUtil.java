package org.frameworkset.tran.ouput.custom;
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

import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.ouput.dummy.DummyOupputConfig;
import org.frameworkset.tran.ouput.dummy.DummyOupputContextImpl;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/10/9 22:32
 * @author biaoping.yin
 * @version 1.0
 */
public class CustomDummyUtil {

	private static ImportContext buildTargetImportContext(BaseImportConfig importConfig){
		DummyOupputContextImpl dummyOupputContext = new DummyOupputContextImpl((DummyOupputConfig) importConfig);
		dummyOupputContext.init();
		return dummyOupputContext;
	}


	public static void setTargetImportContext(DummyOupputConfig dummyOupputConfig,DataStream dataStream){
		ImportContext importContext = dataStream.getImportContext();
		if(dummyOupputConfig != null) {
			dataStream.setTargetImportContext(buildTargetImportContext(dummyOupputConfig));
		}
		else if(importContext.getCustomOutPut() != null){
			dataStream.setTargetImportContext(importContext);
		}
		else
			throw new DataImportException("Must set CustomOutPut or DummyOupputConfig ," +
					"\n Set CustomOutPut  as:\n\t\t//自己处理数据\n" +
					"importBuilder.setCustomOutPut(new CustomOutPut() {\n" +
					"   @Override\n" +
					"   public void handleData(TaskContext taskContext, List<CommonRecord> datas) {\n" +
					"\n" +
					"      //You can do any thing here for datas\n" +
					"      for(CommonRecord record:datas){\n" +
					"         Map<String,Object> data = record.getDatas();\n" +
					"         logger.info(SimpleStringUtil.object2json(data));\n" +
					"      }\n" +
					"   }\n" +
					"});\r\n" +
					"\n Or Set DummyOupputConfig  as:\n\t\tDummyOupputConfig dummyOupputConfig = new DummyOupputConfig();\n" +
					"\t\tdummyOupputConfig.setRecordGenerator(new RecordGenerator() {\n" +
					"\t\t\t@Override\n" +
					"\t\t\tpublic void buildRecord(Context taskContext, CommonRecord record, Writer builder) throws Exception{\n" +
					"\t\t\t\tSimpleStringUtil.object2json(record.getDatas(),builder);\n" +
					"\n" +
					"\t\t\t}\n" +
					"\t\t}).setPrintRecord(true);\n" +
					"\t\timportBuilder.setDummyOupputConfig(dummyOupputConfig);");
	}
}
