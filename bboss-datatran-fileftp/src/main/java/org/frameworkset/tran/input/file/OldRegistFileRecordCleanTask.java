package org.frameworkset.tran.input.file;
/**
 * Copyright 2023 bboss
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

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.file.input.FileInputConfig;
import org.frameworkset.tran.util.StoppedThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;

/**
 * <p>Description: 迁移过期的已完成状态记录</p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/8/4
 * @author biaoping.yin
 * @version 1.0
 */
public class OldRegistFileRecordCleanTask extends StoppedThread {
    private static final Logger logger = LoggerFactory.getLogger(CompleteFileCleanTask.class);
    private FileInputConfig fileInputConfig;
    private ImportContext importContext;
    private FileListenerService fileListenerService;
    private long registLiveTime;
    public OldRegistFileRecordCleanTask(FileInputConfig fileInputConfig, ImportContext importContext,FileListenerService fileListenerService){
        super("OldRegistFileRecordCleanTask-"+importContext.getJobName());
        this.fileInputConfig = fileInputConfig;
        this.registLiveTime = fileInputConfig.getRegistLiveTime();
        this.importContext = importContext;
        this.fileListenerService = fileListenerService;
        this.setDaemon(true);
    }
    

    @Override
    public void run() { 
        do {
            fileListenerService.handleOldRegistRecords(registLiveTime);
            try {
                sleep(fileInputConfig.getScanOldRegistRecordInterval());
            } catch (InterruptedException e) {
                break;
            }
        } while (!stopped);
    }
	
}
