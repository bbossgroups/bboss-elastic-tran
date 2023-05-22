package org.frameworkset.tran.file.monitor;

import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.util.OSInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * @author xutengfei
 * @description
 * @create 2021/3/16
 */
public class FileInodeHandler {
    protected static Logger logger = LoggerFactory.getLogger(FileInodeHandler.class);


    public static String inode(File file){
        return inode(file,true);
    }

    public static String inode(File file,boolean enableInode){
        String fileId = null;
        if(!OSInfo.isWindows() && enableInode){

            try {
                BasicFileAttributes bfa = Files.readAttributes(file.toPath(),BasicFileAttributes.class);
                fileId = bfa.fileKey().toString();
                //(dev=810,ino=20)
                if(fileId !=null){
                    fileId = fileId.substring(1,fileId.length()-1);
                    String arr[] = fileId.split(",");
                    String dev = null,ino = null;
                    for(String one : arr){
                        String[] k_v = one.split("=");
                        if("dev".equals(k_v[0])){
                            dev = k_v[1];
                        }else{
                            ino = k_v[1];
                        }
                    }
                    fileId = dev+"|"+ino;
                }
            } catch (IOException e) {
                logger.error("inode error",e);
            }
        }
        else{
            fileId = change(file.getAbsolutePath());
        }
        return fileId;
    }
    public static File getFileByInode(FileConfig fileConfig, String inode){
//        File logDir = fileConfig.getLogDir();
        File logDir = fileConfig.getRenameFileLogDir();
//        FilenameFilter filter = fileConfig.getFilter();
        File newFile = null;
        if(logDir.exists() && logDir.isDirectory() ){
            File[] files = logDir.listFiles();
            File file = null;
            String fnode = null;
            for(int i = 0; files != null && i < files.length; i ++){
                file = files[i];
                if(file.isFile() && file.exists()) {
                    fnode = linuxInode(file);
                    if(fnode == null)
                        continue;
                    if(inode.equals(fnode)){
                        newFile = file;
                        break;
                    }
                }
            }
        }
        else{
            logger.info("{} must be a directory or must be exists.",fileConfig.getRenameFileSourcePath() );
        }
        return newFile;
    }
    public static String linuxInode(File file){
        String fileId = null;
        if(!OSInfo.isWindows()){

            try {
                BasicFileAttributes bfa = Files.readAttributes(file.toPath(),BasicFileAttributes.class);
                fileId = bfa.fileKey().toString();
                //(dev=810,ino=20)
                if(fileId !=null){
                    fileId = fileId.substring(1,fileId.length()-1);
                    String arr[] = fileId.split(",");
                    String dev = null,ino = null;
                    for(String one : arr){
                        String[] k_v = one.split("=");
                        if("dev".equals(k_v[0])){
                            dev = k_v[1];
                        }else{
                            ino = k_v[1];
                        }
                    }
                    fileId = dev+"|"+ino;
                }
            } catch (IOException e) {
                logger.error("inode error",e);
            }
        }

        return fileId;
    }
    public static String change(String str){
        return str.replaceAll("\\\\","/");
    }
}
