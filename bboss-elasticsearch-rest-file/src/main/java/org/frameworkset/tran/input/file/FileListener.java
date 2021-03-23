package org.frameworkset.tran.input.file;

import org.frameworkset.tran.file.monitor.FileAlterationListenerAdaptor;
import org.frameworkset.tran.file.monitor.FileAlterationObserver;
import org.frameworkset.tran.file.monitor.FileEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * @author xutengfei
 * @description
 * @create 2021/3/15
 */
public class FileListener extends FileAlterationListenerAdaptor {

    private static Logger logger = LoggerFactory.getLogger(FileReaderTask.class);

    private FileListenerService fileListenerService;
    // 采用构造函数注入服务
    public FileListener(FileListenerService fileListenerService) {
        this.fileListenerService = fileListenerService;
    }

    // 文件创建执行
    @Override
    public void onFileCreate(File file) {
        System.out.println(file.getAbsoluteFile()+":onFileCreate");
        // 触发业务
        fileListenerService.doChange(file);
    }

    // 文件创建修改
    @Override
    public void onFileChange(File file) {
        System.out.println(file.getAbsoluteFile()+":onFileChange");
        // 触发业务
        fileListenerService.doChange(file);
    }

    // 文件创建删除
    @Override
    public void onFileDelete(FileEntry entry) {
        System.out.println(entry.getFile().getAbsoluteFile()+":onFileDelete");
        // 文件删除了
        fileListenerService.doDelete(entry);
    }
    //文件移动
    @Override
    public void onFileMove(File oldFile, File newFile) {
        System.out.println(oldFile.getAbsoluteFile()+"->"+newFile.getAbsolutePath()+":onFileMove");
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
        System.out.println(directory.getAbsoluteFile()+":onDirectoryChange");
    }

    // 目录删除
    @Override
    public void onDirectoryDelete(File directory) {
        System.out.println(directory.getAbsoluteFile()+":onDirectoryDelete");
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

    public void reload(){
        fileListenerService.reload();
    }
}
