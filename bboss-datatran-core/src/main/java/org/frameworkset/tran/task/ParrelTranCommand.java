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
import org.frameworkset.tran.TranErrorWrapper;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.metrics.ImportCount;

import java.io.Writer;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * <p>Description: 数据同步并行处理流程指令</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/3/14
 * @author biaoping.yin
 * @version 1.0
 */
public interface ParrelTranCommand {
	/**
	 * 并行处理方法
	 * @param totalCount
	 * @param dataSize
	 * @param taskNo
	 * @param lastValue
	 * @param datas
	 * @param reachEOFClosed
	 * @param record
	 * @param service
	 * @param tasks
	 * @param tranErrorWrapper
	 * @return
	 */
	public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, Object lastValue,
								  Object datas, boolean reachEOFClosed, CommonRecord record, ExecutorService service, List<Future> tasks, TranErrorWrapper tranErrorWrapper);

	/**
	 * 构建数据记录对象
	 * @param context
	 * @param writer
	 * @return
	 * @throws Exception
	 */
	public CommonRecord buildStringRecord(Context context, Writer writer) throws Exception;

	/**
	 * 在作业执行的并行任务全部结束后执行
	 */
	void parrelCompleteAction();

	boolean splitCheck(long totalCount);
}
