package org.frameworkset.tran.plugin.file.output;
/**
 * Copyright 2024 bboss
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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.ftp.FtpConfig;
import org.frameworkset.tran.ftp.FtpTransfer;
import org.frameworkset.tran.ftp.SFTPTransfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/8/9
 */
public class FtpSendFileFunction implements SendFileFunction{
    private Logger logger = LoggerFactory.getLogger(FtpSendFileFunction.class);
    private FileOutputConfig fileOutputConfig;
    private ImportContext importContext;
    public FtpSendFileFunction(ImportContext importContext,FileOutputConfig fileOutputConfig){
        this.fileOutputConfig = fileOutputConfig;
        this.importContext = importContext;
    }
    @Override
    public void sendFile(FileOutputConfig fileOutputConfig, String filePath, String remoteFilePath,boolean resend) {
        long s = System.currentTimeMillis();
        if (fileOutputConfig.getTransferProtocol() == FtpConfig.TRANSFER_PROTOCOL_FTP) {
            FtpTransfer.sendFile(fileOutputConfig, filePath, remoteFilePath);
        } else {
            SFTPTransfer.sendFile(fileOutputConfig, filePath);
        }
        long e = System.currentTimeMillis();
        String msg = null;
        if(!resend) {
            msg = "Send file " + filePath + " to " + remoteFilePath + " complete,耗时:"+(e-s)+"毫秒";
        }
        else{
            msg = "Resend file " + filePath + " to " + remoteFilePath + " complete,耗时:"+(e-s)+"毫秒";
        }  
        logger.info(msg);
        importContext.reportJobMetricLog(msg);
    }

    public String getRemoteFilePath(String name){
        return !fileOutputConfig.isDisableftp() ? SimpleStringUtil.getPath(fileOutputConfig.getRemoteFileDir(), name) : null;
    }
}
