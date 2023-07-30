package org.frameworkset.tran.output.fileftp;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/7/29
 * @author biaoping.yin
 * @version 1.0
 */
public class MaxForceFileThresholdCheck extends Thread{
    private static final Logger logger = LoggerFactory.getLogger(MaxForceFileThresholdCheck.class);
    private FileTransfer fileTransfer;
    private boolean stop;
    public MaxForceFileThresholdCheck(FileTransfer fileTransfer){
        super("MaxForceFileThresholdCheck");
        this.fileTransfer = fileTransfer;
//        this.setDaemon(true);
    }
    public void stopMaxForceFileThresholdCheck(){
        this.stop = true;
        this.interrupt();
        try {
            this.join();
        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
        }
    }
    @Override
    public void run() {
        while (true){
            try {
                if(stop)
                    break;
                sleep(fileTransfer.getMaxForceFileThresholdInterval());
                fileTransfer.maxForceFileThresholdCheck();

            }

            catch (InterruptedException e){
//                logger.warn("FileTransfer maxForceFileThresholdCheck failed:",e);
//                try {
//                    fileTransfer.maxForceFileThresholdCheck();
//                }
//                catch (Throwable throwable){
//                    logger.warn("FileTransfer maxForceFileThresholdCheck from InterruptedException failed :",throwable);
//                }
                break;
            }
            catch (Throwable e){
                logger.warn("FileTransfer maxForceFileThresholdCheck failed:",e);
            }
        }
    }
}
