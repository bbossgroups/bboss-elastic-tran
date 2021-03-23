package org.frameworkset.tran.file.monitor;

import java.io.File;

/**
 * Refactor from commons-io
 * @author xutengfei
 * @description
 * @create 2021/3/23
 */
public class FileAlterationListenerAdaptor implements  FileAlterationListener{
    /**
     * File system observer started checking event.
     *
     * @param observer The file system observer
     */
    @Override
    public void onStart(FileAlterationObserver observer) {

    }

    /**
     * Directory created Event.
     *
     * @param directory The directory created
     */
    @Override
    public void onDirectoryCreate(File directory) {

    }

    /**
     * Directory changed Event.
     *
     * @param directory The directory changed
     */
    @Override
    public void onDirectoryChange(File directory) {

    }

    /**
     * Directory deleted Event.
     *
     * @param directory The directory deleted
     */
    @Override
    public void onDirectoryDelete(File directory) {

    }

    /**
     * File created Event.
     *
     * @param file The file created
     */
    @Override
    public void onFileCreate(File file) {

    }

    /**
     * File changed Event.
     *
     * @param file The file changed
     */
    @Override
    public void onFileChange(File file) {

    }

    /**
     * File deleted Event.
     *
     * @param file The file deleted
     */
    @Override
    public void onFileDelete(FileEntry file) {

    }

    /**
     * File system observer finished checking event.
     *
     * @param observer The file system observer
     */
    @Override
    public void onStop(FileAlterationObserver observer) {

    }

    /**
     * File moved Event.
     *
     * @param oldFile
     * @param newFile
     */
    @Override
    public void onFileMove(File oldFile, File newFile) {

    }
}
