package org.frameworkset.tran.exception;
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

import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.slf4j.Logger;

import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/5/13
 */
public class ImportExceptionUtil {
    public static DataImportException buildDataImportException(ImportContext importContext,String error){
        if(importContext != null) {
            StringBuilder builder = new StringBuilder();
            String jobName = importContext.getJobName();
            String jobId = importContext.getJobId();
            String jobType = importContext.getJobType();
            builder.append("jobName=").append(jobName)
                    .append(",jobId=").append(jobId)
                    .append(",jobType=").append(jobType)
                    .append(",errorinfo=").append(error);
            return new DataImportException(builder.toString());
        }
        else{
            return new DataImportException(error);
        }
        
    }

    public static void loginfo(Logger logger,ImportContext importContext, String loginfo,OutputPlugin outputPlugin){
        if(logger.isInfoEnabled()) {
            if (importContext != null) {
                StringBuilder builder = new StringBuilder();
                String jobName = importContext.getJobName();
                String jobId = importContext.getJobId();
                String jobType = importContext.getJobType();
                builder.append("jobName=").append(jobName)
                        .append(",jobId=").append(jobId)
                        .append(",inputJobType=").append(jobType)
                        .append(",outputJobType=").append(outputPlugin.getJobType())
                        .append(",log:").append(loginfo);
//            return new DataImportException(builder.toString());

                logger.info(builder.toString());

            } else {
                logger.info(loginfo);
            }
        }
    }

    public static DataImportException buildDataImportException(ImportContext importContext, String error, List<Throwable> throwables,OutputPlugin outputPlugin){
        DataImportException dataImportException = null;
        if(importContext != null) {
            StringBuilder builder = new StringBuilder();
            String jobName = importContext.getJobName();
            String jobId = importContext.getJobId();
            String jobType = importContext.getJobType();
            builder.append("jobName=").append(jobName)
                    .append(",jobId=").append(jobId)
                    .append(",inputjobType=").append(jobType)
                    .append(",outputJobType=").append(outputPlugin.getJobType())
                    .append(",errorinfo=").append(error);
            dataImportException = new DataImportException(builder.toString());
           
        }
        else{
            dataImportException = new DataImportException(error);
        }
        for(Throwable throwable :throwables) {
            dataImportException.addSuppressed(throwable);
        }
        return dataImportException;

    }
    public static DataImportException buildDataImportException(OutputPlugin outputPlugin,ImportContext importContext, String error, Throwable throwable){
        if(importContext != null) {
            StringBuilder builder = new StringBuilder();
            String jobName = importContext.getJobName();
            String jobId = importContext.getJobId();
            String jobType = importContext.getJobType();
            builder.append("jobName=").append(jobName)
                    .append(",jobId=").append(jobId)
                    .append(",inputJobType=").append(jobType).append(",outputJobType=").append(outputPlugin.getJobType())
                    .append(",errorinfo=").append(error);
            return new DataImportException(builder.toString(),throwable);
        }
        else{
            return new DataImportException(error,throwable);
        }
        

    }

    public static DataImportException buildDataImportException(ImportContext importContext,Throwable throwable){
        if(importContext != null) {
            StringBuilder builder = new StringBuilder();
            String jobName = importContext.getJobName();
            String jobId = importContext.getJobId();
            String jobType = importContext.getJobType();
            builder.append("jobName=").append(jobName)
                    .append(",jobId=").append(jobId)
                    .append(",inputjobType=").append(jobType)
                    .append(",outputJobType=").append(importContext.getOutputPlugin().getJobType());
            return new DataImportException(builder.toString(),throwable);
        }
        else{
            return new DataImportException(throwable);
        }
       

    }

    public static DataImportException buildDataImportException(String jobName,String jobId,String jobType,String error){
        StringBuilder builder = new StringBuilder();
        builder.append("jobName=").append(jobName)
                .append(",jobId=").append(jobId)
                .append(",jobType=").append(jobType)
                .append(",errorinfo=").append(error);
        return new DataImportException(builder.toString());

    }
    public static DataImportException buildDataImportException(String jobName,String jobId,String jobType,String error,Throwable throwable){
        StringBuilder builder = new StringBuilder();
        builder.append("jobName=").append(jobName)
                .append(",jobId=").append(jobId)
                .append(",jobType=").append(jobType)
                .append(",errorinfo=").append(error);
        return new DataImportException(builder.toString(),throwable);

    }

    public static DataImportException buildDataImportException(String jobName,String jobId,String jobType,Throwable throwable){
        StringBuilder builder = new StringBuilder();
     
        builder.append("jobName=").append(jobName)
                .append(",jobId=").append(jobId)
                .append(",jobType=").append(jobType);
        return new DataImportException(builder.toString(),throwable);

    }
}
