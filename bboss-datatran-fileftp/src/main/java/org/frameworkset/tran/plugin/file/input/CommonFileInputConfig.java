package org.frameworkset.tran.plugin.file.input;

import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.FileListenerService;
import org.frameworkset.tran.input.file.FileReaderTask;
import org.frameworkset.tran.input.other.CommonFileConfig;
import org.frameworkset.tran.input.other.CommonFileReaderTask;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;

import java.io.File;

/**
 * 通用文件采集插件:视频、图片压缩包等
 * @author yin-bp@163.com
 * @description
 * @create 2021/3/12
 */
public class CommonFileInputConfig extends FileInputConfig<CommonFileInputConfig> {

    @Override
    public FileReaderTask buildFileReaderTask(TaskContext taskContext, File file, String fileId, FileConfig fileConfig, long pointer, FileListenerService fileListenerService, BaseDataTran fileDataTran,
											  Status currentStatus , FileInputConfig fileImportConfig ){
        FileReaderTask task = new CommonFileReaderTask(taskContext,file,fileId,(CommonFileConfig) fileConfig,
                fileListenerService,fileDataTran,currentStatus,fileImportConfig);
        return task;
    }
    @Override
    public FileReaderTask buildFileReaderTask(String fileId, Status currentStatus, FileInputConfig fileImportConfig ){
        FileReaderTask task =  new CommonFileReaderTask(fileId,currentStatus,fileImportConfig);
        return task;
    }

}
