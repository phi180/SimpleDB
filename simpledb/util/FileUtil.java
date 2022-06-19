package simpledb.util;

import java.io.File;

public class FileUtil {

    public static void deleteFolder(String folderName) {
        deleteFolder(new File(folderName));
    }

    public static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if(files!=null) {
            for(File f: files) {
                if(f.isDirectory()) {
                    deleteFolder(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }

}
