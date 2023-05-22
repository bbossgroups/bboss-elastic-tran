package org.frameworkset.tran.task;
/**
 * Copyright 2022 bboss
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
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.metrics.ImportCount;

import java.io.Writer;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/3/14
 * @author biaoping.yin
 * @version 1.0
 */
public class BaseSerialTranCommand implements SerialTranCommand {

	@Override
	public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, Object lastValue, Object datas, boolean reachEOFClosed, CommonRecord record) {
		return 0;
	}

	@Override
	public int endSerialActionTask(ImportCount totalCount, long dataSize, int taskNo, Object lastValue, Object datas, boolean reachEOFClosed, CommonRecord record) {
		return 0;
	}

	@Override
	public boolean splitCheck(long totalCount) {
		return false;
	}
//
//	@Override
//	public int splitSerialActionTask(ImportCount totalCount, long dataSize, int taskNo, Object lastValue, Object datas, boolean reachEOFClosed, CommonRecord record) {
//		return 0;
//	}
//
//	@Override
//	public boolean splitCheck(long totalCount) {
//		return false;
//	}

	@Override
	public CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
		return null;
	}
}
