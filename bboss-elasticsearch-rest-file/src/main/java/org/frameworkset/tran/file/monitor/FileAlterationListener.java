package org.frameworkset.tran.file.monitor;

import java.io.File;

/**
 * Refactor from commons-io
 * @author xutengfei
 * @description
 * @create 2021/3/23
 */
public interface FileAlterationListener {
    /**
     * File system observer started checking event.
     *
     * @param observer The file system observer
     */
    void onStart(final FileAlterationObserver observer);

    /**
     * Directory created Event.
     *
     * @param directory The directory created
     */
    void onDirectoryCreate(final File directory);

    /**
     * Directory changed Event.
     *
     * @param directory The directory changed
     */
    void onDirectoryChange(final File directory);

    /**
     * Directory deleted Event.
     *
     * @param directory The directory deleted
     */
    void onDirectoryDelete(final File directory);

    /**
     * File created Event.
     *
     * @param file The file created
     */
    void onFileCreate(final File file);

    /**
     * File changed Event.
     *
     * @param file The file changed
     */
    void onFileChange(final File file);

    /**
     * File deleted Event.
     *
     * @param file The file deleted
     */
    void onFileDelete(final FileEntry file);

    /**
     * File system observer finished checking event.
     *
     * @param observer The file system observer
     */
    void onStop(final FileAlterationObserver observer);

    /**
     * File moved Event.
     * @param oldFile
     * @param newFile
     */
    void onFileMove(File oldFile, File newFile);
}
