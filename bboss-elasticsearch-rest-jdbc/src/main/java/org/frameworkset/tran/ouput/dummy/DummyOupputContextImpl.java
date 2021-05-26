package org.frameworkset.tran.ouput.dummy;
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
import org.frameworkset.tran.context.BaseImportContext;
import org.frameworkset.tran.util.JsonReocordGenerator;

import java.io.Writer;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/28 16:48
 * @author biaoping.yin
 * @version 1.0
 */
public class DummyOupputContextImpl extends BaseImportContext implements DummyOupputContext {
	private DummyOupputConfig dummyOupputConfig;
	public DummyOupputContextImpl(DummyOupputConfig dummyOupputConfig){
		super(dummyOupputConfig);
		this.dummyOupputConfig = dummyOupputConfig;

	}
	public void generateReocord(org.frameworkset.tran.context.Context taskContext, CommonRecord record, Writer builder)  throws Exception{
		dummyOupputConfig.getReocordGenerator().buildRecord(  taskContext, record,  builder);
	}
	@Override
	public void init(){
		super.init();
		if(dummyOupputConfig.getReocordGenerator() == null){
			dummyOupputConfig.setReocordGenerator(new JsonReocordGenerator());
		}

	}


	@Override
	public boolean isPrintRecord() {
		return dummyOupputConfig.isPrintRecord();
	}
}
