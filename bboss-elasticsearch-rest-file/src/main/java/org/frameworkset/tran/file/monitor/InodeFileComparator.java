package org.frameworkset.tran.file.monitor;
import java.io.File;
import java.io.Serializable;
import java.util.Comparator;

/**
 * @author xutengfei
 * @description
 * @create 2021/3/23
 */
public class InodeFileComparator implements Comparator<File>, Serializable {

    public int compare(final File file1, final File file2) {
        return FileInodeHandler.inode(file1).compareTo(FileInodeHandler.inode(file2));
    }
}
