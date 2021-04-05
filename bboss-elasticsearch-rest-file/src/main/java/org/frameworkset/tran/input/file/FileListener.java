package org.frameworkset.tran.input.file;

import org.frameworkset.tran.file.monitor.FileAlterationListenerAdaptor;
import org.frameworkset.tran.file.monitor.FileAlterationObserver;
import org.frameworkset.tran.file.monitor.FileEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * @author xutengfei,yin-bp@163.com
 * @description
 * @create 2021/3/15
 */
public class FileListener extends FileAlterationListenerAdaptor {

    private static Logger logger = LoggerFactory.getLogger(FileListener.class);

    private FileListenerService fileListenerService;
    // 采用构造函数注入服务
    public FileListener(FileListenerService fileListenerService) {
        this.fileListenerService = fileListenerService;
    }

    // 文件创建执行
    @Override
    public void onFileCreate(File file) {
        if(logger.isInfoEnabled())
            logger.info(file.getAbsoluteFile()+":onFileCreate");
        // 触发业务
        fileListenerService.doChange(file);
    }

    // 文件创建修改
    @Override
    public void onFileChange(File file) {
        if(logger.isInfoEnabled())
            logger.info(file.getAbsoluteFile()+":onFileChange");
        // 触发业务
        fileListenerService.doChange(file);
    }

    // 文件创建删除
    @Override
    public void onFileDelete(FileEntry entry) {
        if(logger.isInfoEnabled())
            logger.info(entry.getFile().getAbsoluteFile()+":onFileDelete");
        // 文件删除了
        fileListenerService.doDelete(entry);
    }
    //文件移动
    @Override
    public void onFileMove(File oldFile, File newFile) {
        if(logger.isInfoEnabled())
            logger.info(oldFile.getAbsoluteFile()+"->"+newFile.getAbsolutePath()+":onFileMove");
        fileListenerService.onFileMove(oldFile, newFile);
    }

    // 目录创建
    @Override
    public void onDirectoryCreate(File directory) {
//        System.out.println(directory.getAbsoluteFile()+":onDirectoryCreate");
    }

    // 目录修改
    @Override
    public void onDirectoryChange(File directory) {
        if(logger.isInfoEnabled())
            logger.info(directory.getAbsoluteFile()+":onDirectoryChange");
    }

    // 目录删除
    @Override
    public void onDirectoryDelete(File directory) {
        if(logger.isInfoEnabled())
            logger.info(directory.getAbsoluteFile()+":onDirectoryDelete");
    }


    // 轮询开始
    @Override
    public void onStart(FileAlterationObserver observer) {
//        System.out.println("onStart");
    }

    // 轮询结束
    @Override
    public void onStop(FileAlterationObserver observer) {
//        System.out.println("onStop");
    }

//    public void reload(){
//        fileListenerService.reload();
//    }

    public FileListenerService getFileListenerService() {
        return fileListenerService;
    }

    public void checkTranFinished() {
        fileListenerService.checkTranFinished();
    }
}
