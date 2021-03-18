package org.frameworkset.tran.input.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * @author xutengfei
 * @description
 * @create 2021/3/16
 */
public class FileInodeHandler {
    protected static Logger logger = LoggerFactory.getLogger(FileInodeHandler.class);
    public static String inode(File file){
        String fileId = change(file.getAbsolutePath());
        if(System.getProperty("os.name").indexOf("Linux")>=0){
            //获取文件号
            Path path = Paths.get(fileId);
            try {
                BasicFileAttributes bfa = Files.readAttributes(path,BasicFileAttributes.class);
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
                System.err.println(e.getMessage());
            }
        }
        return fileId;
    }
    public static String change(String str){
        return str.replaceAll("\\\\","/");
    }
}
