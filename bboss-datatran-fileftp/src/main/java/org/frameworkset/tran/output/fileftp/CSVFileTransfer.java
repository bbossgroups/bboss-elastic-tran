package org.frameworkset.tran.output.fileftp;
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

import com.opencsv.CSVWriter;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.plugin.file.output.CSVFileOutputConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.util.DataFormatUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * CSV文件输出实现类 
 * 文件切割记录规则：达到最大记录数或者空闲时间达到最大空闲时间阈值，进行文件切割
 * 如果不切割文件，达到最大最大空闲时间阈值，当切割文件标识为false时，只执行flush数据操作，不关闭文件也不生成新的文件，否则生成新的文件
 *
 * </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/29 10:25
 * @author biaoping.yin
 * @version 1.0
 */
public class CSVFileTransfer extends FileTransfer{
    private static Logger logger = LoggerFactory.getLogger(CSVFileTransfer.class);
    private CSVFileOutputConfig csvFileOutputConfig;
    private CSVWriter csvWriter;
    public CSVFileTransfer(CSVFileOutputConfig csvFileOutputConfig, String dir, FileFtpOutPutDataTran fileFtpOutPutDataTran) {
        super(csvFileOutputConfig,  dir,  fileFtpOutPutDataTran);
        this.csvFileOutputConfig = csvFileOutputConfig;
    }

    protected void initTransfer() throws IOException {
        super.initTransfer();
        csvWriter = new CSVWriter(bw);
    }
	/**
	 * 添加标题行
	 * @throws Exception
	 */
	public void writeHeader() throws Exception {

        if(this.csvFileOutputConfig.isEnableHeader()) {
            CSVRecordGenerator.buildHeaderRecord(this.csvFileOutputConfig, csvWriter);
        }
	}
	

    public void writeData(TaskCommand taskCommand, List<CommonRecord> datas, TaskContext taskContext, TaskMetrics taskMetrics) throws Exception {
        transferLock.lock();
        try {
            DataFormatUtil.initDateformatThreadLocal();
            CommonRecord record = null;
            long count = 0;
            
            for(int i = 0; i < datas.size(); i ++){
                record = datas.get(i);
                init();
                CSVRecordGenerator.buildRecord(this.csvFileOutputConfig,record,csvWriter);
                refreshLastWriteDataTime();
                count = records.increamentUnSynchronized();
                if (maxFileRecordSize > 0L) {
                    boolean reachMaxedSize = count >= maxFileRecordSize;
                    if (reachMaxedSize) {  
                        count = 0;
                        try {
                            sendFile();
                        }
                        finally {
                            reset();                            
                        }
                    }
                }
            }
            if(count > 0){
                bw.flush();
            }
          
        }
        finally {
            
            transferLock.unlock();
            DataFormatUtil.releaseDateformatThreadLocal();
        }

    }

    @Override
    public void close(){

        super.close();
        if(csvWriter != null) {
            try {
                csvWriter.close();
            } catch (Exception e) {
            }
            csvWriter = null;
        }
    }
 
 
}
