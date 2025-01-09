package org.frameworkset.tran.output.excelftp;
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
import org.frameworkset.tran.output.fileftp.FileFtpOutPutDataTran;
import org.frameworkset.tran.output.fileftp.FileTransfer;
import org.frameworkset.tran.plugin.file.output.ExcelFileOutputConfig;
import org.frameworkset.tran.task.TaskCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * <p>Description: 生产excel文件，可以根据配置将生成的文件发送到ftp服务器</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/29 10:25
 * @author biaoping.yin
 * @version 1.0
 */
public class ExcelFileTransfer extends FileTransfer {
    private static Logger logger = LoggerFactory.getLogger(ExcelFileTransfer.class);
	private ExportExcelUtil exportExcelUtil;
	private ExcelFileOutputConfig excelFileOutputConfig;
	public ExcelFileTransfer(ExcelFileOutputConfig excelFileOutputConfig, String dir, FileFtpOutPutDataTran fileFtpOutPutDataTran) {
		super(excelFileOutputConfig,  dir,  fileFtpOutPutDataTran);
		this.excelFileOutputConfig = excelFileOutputConfig;
	}

	@Override
	public void initTransfer() throws IOException {
        if(!file.exists())
            file.createNewFile();
		exportExcelUtil = new ExportExcelUtil();
		exportExcelUtil.initialize(excelFileOutputConfig.getFlushRows());
		exportExcelUtil.buildSheet(excelFileOutputConfig);
	}

	@Override
	public void writeHeader() throws Exception {

	}
	public  void writeData(TaskCommand taskCommand, List<CommonRecord> datas) throws IOException {
        transferLock.lock();
        try {
            init();
//            if(records != null) {
//                long dataSize = datas != null ? datas.size():0L ;
//                 
//                records.increamentUnSynchronized(dataSize);
//            }
//            , new SplitFunction(this) {
//
//                @Override
//                public boolean split(long count) {
//                    boolean reachMaxedSize = false;
//                    if (maxFileRecordSize > 0L) {
//                        reachMaxedSize = count >= maxFileRecordSize;
//                        if (reachMaxedSize) {
//                            try {
//                                sendFile();
//                            } finally {
//                                reset();
//                            }
//                        }
//                    }
//                    return reachMaxedSize;
//                }
//            }
            long count = 0;
            for(CommonRecord commonRecord:datas) {
                init();
                exportExcelUtil.writeData(commonRecord);
                count = records.increamentUnSynchronized();
                
                refreshLastWriteDataTime();
                boolean reachMaxedSize = false;
                if (maxFileRecordSize > 0L) {
                    reachMaxedSize = count >= maxFileRecordSize;
                    if (reachMaxedSize) {
                        count = 0;
                        try {
                            sendFile();
                        } finally {
                            reset();
                        }
                    }
                }
            }
            if(count > 0){
                
            }
            
            /**
            this.lastWriteDataTime = System.currentTimeMillis();
            if (maxFileRecordSize > 0L) {
                boolean reachMaxedSize = records.getCountUnSynchronized() >= maxFileRecordSize;
                if (reachMaxedSize) {
                    try {
                        sendFile();
                    }
                    finally {
                        reset();
                    }
                }
            }*/
        }
        finally {
            transferLock.unlock();
        }

	}
	
    @Override
	protected void flush(boolean close) throws IOException {
		exportExcelUtil.write(file);
        if(close)
		    this.close();
	}

	@Override
	public void close(){
		if(exportExcelUtil != null) {
            exportExcelUtil.dispose();
            exportExcelUtil = null;
        }

	}

    public long increamentUnSynchronized() {
        return records.increamentUnSynchronized();
    }
}
