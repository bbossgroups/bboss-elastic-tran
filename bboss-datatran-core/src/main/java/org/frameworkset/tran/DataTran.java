package org.frameworkset.tran;
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

import org.frameworkset.elasticsearch.scroll.BreakableScrollHandler;
import org.frameworkset.soa.BBossStringWriter;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/27 9:50
 * @author biaoping.yin
 * @version 1.0
 */
public interface DataTran {

	public void beforeOutputData(BBossStringWriter writer);
	/**
	 * 并行批处理导入

	 * @return
	 */
	String parallelBatchExecute(  );
	public AsynTranResultSet getAsynTranResultSet();
	/**
	 * 串行批处理导入

	 * @return
	 */
	String batchExecute(  );

	/**
	 * 逐条导入
	 * @return
	 */
	String serialExecute(   ) throws DataImportException;
	public BreakableScrollHandler getBreakableScrollHandler();
	void waitTasksComplete(final List<Future> tasks,
						   final ExecutorService service, Exception exception, Object lastValue, final ImportCount totalCount ,
						   final TranErrorWrapper tranErrorWrapper ,WaitTasksCompleteCallBack waitTasksCompleteCallBack,boolean reachEOFClosed);
	ImportContext getImportContext();
}
