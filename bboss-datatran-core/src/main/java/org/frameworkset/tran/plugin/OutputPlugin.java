package org.frameworkset.tran.plugin;
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

import org.frameworkset.tran.*;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.JobTaskMetrics;
import org.frameworkset.tran.record.RecordColumnInfo;
import org.frameworkset.tran.record.RecordOutpluginSpecialConfig;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/18
 * @author biaoping.yin
 * @version 1.0
 */
public interface OutputPlugin {
	ImportContext getImportContext() ;
    RecordColumnInfo resolveRecordColumnInfo(Object value, FieldMeta fieldMeta, Context context);
	BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet tranResultSet, JobCountDownLatch countDownLatch, Status currentStatus);
	void afterInit();
	void beforeInit();
	void init();
	void setDataTranPlugin(DataTranPlugin dataTranPlugin);
	void destroy(boolean waitTranStop);
    String getJobType();
    OutputConfig getOutputConfig();
	JobTaskMetrics createJobTaskMetrics();

    default void stopCollectData(){

	}

    CommonRecord buildRecord(Context context) throws Exception;

    /**
     * 创建内部转换器
     * @param baseDataTran
     * @return
     */
    BaseDataTran createBaseDataTran(BaseDataTran baseDataTran) ;

    void buildRecordOutpluginSpecialConfig(CommonRecord dataRecord, Context context) throws Exception;
}
