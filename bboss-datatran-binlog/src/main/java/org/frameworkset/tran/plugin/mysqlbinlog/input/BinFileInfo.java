package org.frameworkset.tran.plugin.mysqlbinlog.input;

public class BinFileInfo {
    private String fileName;
    private Long fileSize;

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public Long getFileSize() {
        return fileSize;
    }

    public void setFileSize(Long fileSize) {
        this.fileSize = fileSize;
    }
    @Override
    public String toString(){
        if(fileSize != null)
            return fileName+"/"+fileSize;
        else{
            return fileName;
        }
    }
}
