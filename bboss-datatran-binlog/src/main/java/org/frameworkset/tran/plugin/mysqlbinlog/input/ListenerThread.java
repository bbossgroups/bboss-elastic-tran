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

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import org.frameworkset.tran.DataImportException;

import java.io.IOException;

/**
 * <p>Description: 启动mysql binlog client线程</p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/6/28
 * @author biaoping.yin
 * @version 1.0
 */
public class ListenerThread extends Thread{
    private BinaryLogClient client;
    private DataImportException dataImportException ;

    public ListenerThread(BinaryLogClient client){
        this.client = client;
        setName("BinaryLogClient-connect-Thread");
//        this.setDaemon(true);
    }
        @Override
    public void run() {
        try {
            client.connect();
        } catch (IOException e) {
            dataImportException = new DataImportException(e);
        }
        catch (Exception e) {
            dataImportException = new DataImportException(e);
        }
        catch (Throwable e) {
            dataImportException =  new DataImportException(e);
        }
    }

    public DataImportException getDataImportException() {
        return dataImportException;
    }
}
