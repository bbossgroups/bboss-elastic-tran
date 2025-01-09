package org.frameworkset.tran.task;
/**
 * Copyright 2025 bboss
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
import org.frameworkset.tran.TranErrorWrapper;
import org.frameworkset.tran.WrapedExportResultHandler;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2025/1/8
 */
public class MultiOutputTaskCall extends TaskCall{
    private static final Logger log = LoggerFactory.getLogger(MultiOutputTaskCall.class); 
    public MultiOutputTaskCall(TaskCommand taskCommand, TranErrorWrapper errorWrapper) {
        super(taskCommand, errorWrapper);
    }

    @Override
    protected  <RESULT> RESULT innerCall(TaskCommand<RESULT> taskCommand){
        ImportContext importContext = taskCommand.getImportContext();
        try {
            RESULT data = taskCommand.execute();
            WrapedExportResultHandler wrapedExportResultHandler = importContext.getExportResultHandler(taskCommand.getOutputConfig());
            if (wrapedExportResultHandler != null) {//处理返回值
                try {
                    wrapedExportResultHandler.handleResult(taskCommand, data);
                }
                catch (Exception e){
                    StringBuilder stringBuilder = BaseTranJob.builderJobInfo(importContext.getInputPlugin(), taskCommand.getOutputPlugin(), new StringBuilder(),  importContext);
                    log.warn(stringBuilder.toString(),e);
                }
            }
            return data;
        }
        catch (DataImportException e){
            exportResultHandler(  taskCommand,  importContext,  e);
            throw e;
        }
        catch (Exception e){
            exportResultHandler(  taskCommand,  importContext,  e);
            throw ImportExceptionUtil.buildDataImportException(importContext,e);
        }
        catch (Throwable e){
            exportResultHandler(  taskCommand,  importContext,  e);
            throw ImportExceptionUtil.buildDataImportException(importContext,e);
        }
//        finally {
//            taskCommand.finished();
//        }


    }
}
