package org.frameworkset.tran.plugin.file.input;

import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.FileListenerService;
import org.frameworkset.tran.input.file.FileReaderTask;
import org.frameworkset.tran.input.word.WordFileConfig;
import org.frameworkset.tran.input.word.WordFileReaderTask;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;

import java.io.File;

/**
 * @author yin-bp@163.com
 * @description
 * @create 2021/3/12
 */
public class WordFileInputConfig extends FileInputConfig<WordFileInputConfig> {

    @Override
    public FileReaderTask buildFileReaderTask(TaskContext taskContext, File file, String fileId, FileConfig fileConfig, long pointer, FileListenerService fileListenerService, BaseDataTran fileDataTran,
											  Status currentStatus , FileInputConfig fileImportConfig ){
        FileReaderTask task = new WordFileReaderTask(taskContext,file,fileId,(WordFileConfig)fileConfig,
                fileListenerService,fileDataTran,currentStatus,fileImportConfig);
        return task;
    }
    @Override
    public FileReaderTask buildFileReaderTask(String fileId, Status currentStatus, FileInputConfig fileImportConfig ){
        FileReaderTask task =  new WordFileReaderTask(fileId,currentStatus,fileImportConfig);
        return task;
    }

}
