package org.frameworkset.tran.plugin.mysqlbinlog.input;
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

import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;

import java.io.IOException;

/**
 * <p>Description: 启动mysql binlog client线程</p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/6/28
 * @author biaoping.yin
 * @version 1.0
 */
public class ClientConnectThread extends Thread{
    private static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ClientConnectThread.class);
    private BinaryLogClientExt client;
    private DataImportException dataImportException ;
    protected ImportContext importContext;
    public ClientConnectThread(ImportContext importContext,BinaryLogClientExt client){
        this.client = client;
        this.importContext = importContext;
        setName("BinaryLogClient-connect-Thread");
//        this.setDaemon(true);
    }
        @Override
    public void run() {
        try {
            log.info("Start BinaryLogClientExt begin.");
            client.connect();
            log.info("Start BinaryLogClientExt complete.");
        } catch (IOException e) {
            dataImportException = ImportExceptionUtil.buildDataImportException(importContext,e);
        }
        catch (Exception e) {
            dataImportException = ImportExceptionUtil.buildDataImportException(importContext,e);
        }
        catch (Throwable e) {
            dataImportException =  ImportExceptionUtil.buildDataImportException(importContext,e);
        }
    }

    public DataImportException getDataImportException() {
        return dataImportException;
    }
}
