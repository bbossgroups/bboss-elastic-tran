package org.frameworkset.tran.input.file;

/**
 * @author xutengfei
 * @description
 * @create 2021/3/12
 */
public class FileConfig {
    //文件监听路径
    private String sourcePath;
    //文件名称正则匹配
    private String fileNameRegular;
    //文件换行标识符，以什么开头,正则匹配
    private String fileHeadLineRegular;
    //是否检测子目录
    private boolean scanChild;
    public FileConfig() {
    }
    public FileConfig(String sourcePath, String fileNameRegular, String fileHeadLineRegular) {
        this.sourcePath = sourcePath;
        this.fileNameRegular = fileNameRegular;
        this.fileHeadLineRegular = fileHeadLineRegular;
    }
    public FileConfig(String sourcePath, String fileNameRegular, String fileHeadLineRegular, boolean scanChild) {
        this.sourcePath = sourcePath;
        this.fileNameRegular = fileNameRegular;
        this.fileHeadLineRegular = fileHeadLineRegular;
        this.scanChild = scanChild;
    }
    public String getSourcePath() {
        return sourcePath;
    }

    public void setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
    }

    public String getFileNameRegular() {
        return fileNameRegular;
    }

    public void setFileNameRegular(String fileNameRegular) {
        this.fileNameRegular = fileNameRegular;
    }

    public String getFileHeadLineRegular() {
        return fileHeadLineRegular;
    }

    public void setFileHeadLineRegular(String fileHeadLineRegular) {
        this.fileHeadLineRegular = fileHeadLineRegular;
    }

    public boolean isScanChild() {
        return scanChild;
    }
}
