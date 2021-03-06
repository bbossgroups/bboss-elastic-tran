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

import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.util.RecordGenerator;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/28 16:48
 * @author biaoping.yin
 * @version 1.0
 */
public class DummyOupputConfig extends BaseImportConfig {
	private boolean printRecord;
	/**
	 * 输出文件记录处理器:org.frameworkset.tran.util.ReocordGenerator
	 */
	private RecordGenerator recordGenerator;
	public boolean isPrintRecord() {
		return printRecord;
	}

	public DummyOupputConfig setPrintRecord(boolean printRecord) {
		this.printRecord = printRecord;
		return this;
	}

	public RecordGenerator getRecordGenerator() {
		return recordGenerator;
	}

	public DummyOupputConfig setRecordGenerator(RecordGenerator recordGenerator) {
		this.recordGenerator = recordGenerator;
		return this;
	}
}
