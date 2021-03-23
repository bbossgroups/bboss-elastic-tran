package org.frameworkset.tran.file.monitor;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileFilter;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Refactor from commons-io
 * @author xutengfei
 * @description
 * @create 2021/3/23
 */
public class FileAlterationObserver implements Serializable {

    private static final long serialVersionUID = 3185122225658782848L;
    private final List<FileAlterationListener> listeners = new CopyOnWriteArrayList<>();
    private final FileEntry rootEntry;
    private final FileFilter fileFilter;
    private final Comparator<File> comparator;

    /**
     * Constructs an observer for the specified directory.
     *
     * @param directoryName the name of the directory to observe
     */
    public FileAlterationObserver(final String directoryName) {
        this(new File(directoryName));
    }

    /**
     * Constructs an observer for the specified directory and file filter.
     *
     * @param directoryName the name of the directory to observe
     * @param fileFilter The file filter or null if none
     */
    public FileAlterationObserver(final String directoryName, final FileFilter fileFilter) {
        this(new File(directoryName), fileFilter);
    }
    /**
     * Constructs an observer for the specified directory.
     *
     * @param directory the directory to observe
     */
    public FileAlterationObserver(final File directory) {
        this(directory, null);
    }
    /**
     * Constructs an observer for the specified directory, file filter and
     * file comparator.
     *
     * @param directory the directory to observe
     * @param fileFilter The file filter or null if none
     */
    public FileAlterationObserver(final File directory, final FileFilter fileFilter) {
        this(new FileEntry(directory), fileFilter);
    }

    /**
     * Constructs an observer for the specified directory, file filter and
     * file comparator.
     *
     * @param rootEntry the root directory to observe
     * @param fileFilter The file filter or null if none
     */
    protected FileAlterationObserver(final FileEntry rootEntry, final FileFilter fileFilter) {
        if (rootEntry == null) {
            throw new IllegalArgumentException("Root entry is missing");
        }
        if (rootEntry.getFile() == null) {
            throw new IllegalArgumentException("Root directory is missing");
        }
        this.rootEntry = rootEntry;
        this.fileFilter = fileFilter;
        this.comparator = new InodeFileComparator();
    }

    /**
     * Returns the directory being observed.
     *
     * @return the directory being observed
     */
    public File getDirectory() {
        return rootEntry.getFile();
    }

    /**
     * Returns the fileFilter.
     *
     * @return the fileFilter
     * @since 2.1
     */
    public FileFilter getFileFilter() {
        return fileFilter;
    }

    /**
     * Adds a file system listener.
     *
     * @param listener The file system listener
     */
    public void addListener(final FileAlterationListener listener) {
        if (listener != null) {
            listeners.add(listener);
        }
    }

    /**
     * Removes a file system listener.
     *
     * @param listener The file system listener
     */
    public void removeListener(final FileAlterationListener listener) {
        if (listener != null) {
            while (listeners.remove(listener)) {
                // empty
            }
        }
    }

    /**
     * Returns the set of registered file system listeners.
     *
     * @return The file system listeners
     */
    public Iterable<FileAlterationListener> getListeners() {
        return listeners;
    }

    /**
     * Initializes the observer.
     *
     * @throws Exception if an error occurs
     */
    public void initialize() throws Exception {
        rootEntry.refresh(rootEntry.getFile());
        final FileEntry[] children = doListFiles(rootEntry.getFile(), rootEntry);
        rootEntry.setChildren(children);
    }

    /**
     * Final processing.
     *
     * @throws Exception if an error occurs
     */
    public void destroy() throws Exception {
        // noop
    }

    /**
     * Checks whether the file and its children have been created, modified or deleted.
     */
    public void checkAndNotify() {

        /* fire onStart() */
        for (final FileAlterationListener listener : listeners) {
            listener.onStart(this);
        }

        /* fire directory/file events */
        final File rootFile = rootEntry.getFile();
        if (rootFile.exists()) {
            checkAndNotify(rootEntry, rootEntry.getChildren(), listFiles(rootFile));
        } else if (rootEntry.isExists()) {
            checkAndNotify(rootEntry, rootEntry.getChildren(), FileUtils.EMPTY_FILE_ARRAY);
        } else {
            // Didn't exist and still doesn't
        }

        /* fire onStop() */
        for (final FileAlterationListener listener : listeners) {
            listener.onStop(this);
        }
    }

    /**
     * Compares two file lists for files which have been created, modified or deleted.
     *
     * @param parent The parent entry
     * @param previous The original list of files
     * @param files  The current list of files
     */
    private void checkAndNotify(final FileEntry parent, final FileEntry[] previous, final File[] files) {
        int c = 0;
        final FileEntry[] current = files.length > 0 ? new FileEntry[files.length] : FileEntry.EMPTY_ENTRIES;
        for (final FileEntry entry : previous) {
            while (c < files.length && comparator.compare(entry.getFile(), files[c]) > 0) {
                current[c] = createFileEntry(parent, files[c]);
                doCreate(current[c]);
                c++;
            }
            if (c < files.length && comparator.compare(entry.getFile(), files[c]) == 0) {
                doMatch(entry, files[c]);
                checkAndNotify(entry, entry.getChildren(), listFiles(files[c]));
                current[c] = entry;
                c++;
            } else {
                checkAndNotify(entry, entry.getChildren(), FileUtils.EMPTY_FILE_ARRAY);
                doDelete(entry,files);
            }
        }
        for (; c < files.length; c++) {
            current[c] = createFileEntry(parent, files[c]);
            doCreate(current[c]);
        }
        parent.setChildren(current);
    }

    /**
     * Creates a new file entry for the specified file.
     *
     * @param parent The parent file entry
     * @param file The file to create an entry for
     * @return A new file entry
     */
    private FileEntry createFileEntry(final FileEntry parent, final File file) {
        final FileEntry entry = parent.newChildInstance(file);
        entry.refresh(file);
        final FileEntry[] children = doListFiles(file, entry);
        entry.setChildren(children);
        return entry;
    }

    /**
     * Lists the files
     * @param file The file to list files for
     * @param entry the parent entry
     * @return The child files
     */
    private FileEntry[] doListFiles(final File file, final FileEntry entry) {
        final File[] files = listFiles(file);
        final FileEntry[] children = files.length > 0 ? new FileEntry[files.length] : FileEntry.EMPTY_ENTRIES;
        for (int i = 0; i < files.length; i++) {
            children[i] = createFileEntry(entry, files[i]);
        }
        return children;
    }

    /**
     * Fires directory/file created events to the registered listeners.
     *
     * @param entry The file entry
     */
    private void doCreate(final FileEntry entry) {
        for (final FileAlterationListener listener : listeners) {
            if (entry.isDirectory()) {
                listener.onDirectoryCreate(entry.getFile());
            } else {
                listener.onFileCreate(entry.getFile());
            }
        }
        final FileEntry[] children = entry.getChildren();
        for (final FileEntry aChildren : children) {
            doCreate(aChildren);
        }
    }

    /**
     * Fires directory/file change events to the registered listeners.
     *  inode相同的时候会匹配上，但是可能是重命名了 肯定是文件Innode相同才会走这里
     * @param entry The previous file system entry
     * @param file The current file
     */
    private void doMatch(final FileEntry entry, final File file) {
        //fileId相同，但是文件名称不同，说明是重命名了
        if(!file.getName().equals(entry.getName()) && !entry.isDirectory()){
            for (final FileAlterationListener listener : listeners) {
                listener.onFileMove(entry.getFile(),file);
            }
        }
        if (entry.refresh(file)) {
            for (final FileAlterationListener listener : listeners) {
                if (entry.isDirectory()) {
                    listener.onDirectoryChange(file);
                } else {
                    listener.onFileChange(file);
                }
            }
        }
    }
    /**
     * Fires directory/file delete events to the registered listeners.
     *
     * @param entry The file entry
     * @param files
     */
    private void doDelete(final FileEntry entry, File[] files) {
        for (final FileAlterationListener listener : listeners) {
            if (entry.isDirectory()) {
                listener.onDirectoryDelete(entry.getFile());
            } else {
                boolean move = false;
                for(File file: files){
                    if(entry.getFileId().equals(FileInodeHandler.inode(file))){
                        move = true;
                        listener.onFileMove(entry.getFile(),file);
                        break;
                    }
                }
                if(!move){
                    listener.onFileDelete(entry);
                }
            }
        }
    }

    /**
     * Lists the contents of a directory
     *
     * @param file The file to list the contents of
     * @return the directory contents or a zero length array if
     * the empty or the file is not a directory
     */
    private File[] listFiles(final File file) {
        File[] children = null;
        if (file.isDirectory()) {
            children = fileFilter == null ? file.listFiles() : file.listFiles(fileFilter);
        }
        if (children == null) {
            children = FileUtils.EMPTY_FILE_ARRAY;
        }
        if (comparator != null && children.length > 1) {
            Arrays.sort(children, comparator);
        }
        return children;
    }

    /**
     * Returns a String representation of this observer.
     *
     * @return a String representation of this observer
     */
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append(getClass().getSimpleName());
        builder.append("[file='");
        builder.append(getDirectory().getPath());
        builder.append('\'');
        if (fileFilter != null) {
            builder.append(", ");
            builder.append(fileFilter.toString());
        }
        builder.append(", listeners=");
        builder.append(listeners.size());
        builder.append("]");
        return builder.toString();
    }

}
