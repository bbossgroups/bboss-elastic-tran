package org.frameworkset.tran.input.file;

import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationObserver;

import java.io.File;

/**
 * @author xutengfei
 * @description
 * @create 2021/3/15
 */
public class FileListener extends FileAlterationListenerAdaptor {

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
    public void onFileDelete(File file) {
        System.out.println(file.getAbsoluteFile()+":onFileDelete");
        // 删除不处理
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
