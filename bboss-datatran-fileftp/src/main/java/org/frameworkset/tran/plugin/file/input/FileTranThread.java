package org.frameworkset.tran.plugin.file.input;
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

import org.frameworkset.tran.AssertMaxThreshold;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.schedule.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/4/7
 * @author biaoping.yin
 * @version 1.0
 */
public class FileTranThread extends Thread{
    protected static Logger logger = LoggerFactory.getLogger(FileTranThread.class);

    private AssertMaxThreshold assertMaxFilesThreshold;

    private TaskContext taskContext;
    private BaseDataTran fileDataTran;
    private FileDataTranPluginImpl fileDataTranPlugin;
    public FileTranThread(AssertMaxThreshold assertMaxFilesThreshold,
                           BaseDataTran fileDataTran,
                           TaskContext taskContext,FileDataTranPluginImpl fileDataTranPlugin){
        this.assertMaxFilesThreshold = assertMaxFilesThreshold;
        this.taskContext = taskContext;
        this.fileDataTran = fileDataTran;
        this.fileDataTranPlugin = fileDataTranPlugin;
    }
    private boolean runned;

    public synchronized boolean startTran() {
        super.start();
        return isRun();
    }

    @Override
    public void run() {
        try {
//            if(assertMaxFilesThreshold != null)
//                assertMaxFilesThreshold.increament();
            runned = true;
            fileDataTran.tran();
        }
        catch (DataImportException dataImportException){
            logger.error("",dataImportException);
            fileDataTranPlugin.throwException(  taskContext,  dataImportException);
            fileDataTran.stop();
        }
        catch (RuntimeException dataImportException){
            logger.error("",dataImportException);
            fileDataTranPlugin.throwException(  taskContext,  dataImportException);
            fileDataTran.stop();
        }
        catch (Throwable dataImportException){
            logger.error("",dataImportException);
            DataImportException dataImportException_ = new DataImportException(dataImportException);
            fileDataTranPlugin.throwException(  taskContext, dataImportException_);
            fileDataTran.stop();
        }
        finally {
            if(assertMaxFilesThreshold != null )
                assertMaxFilesThreshold.decreament();
        }

    }
    public boolean isRun(){
        do {
            if(runned)
                break;
            try {
                sleep(500L);
            } catch (InterruptedException e) {
                break;
            }
        }while (true);
        return runned;


    }
}
