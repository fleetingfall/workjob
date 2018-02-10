package scala.spyder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * @Authork:kingcall
 * @Description:
 * @Date:$time$ $date$
 */
public class JavaHelper {
    public static void download(String path, String fileName, String fileUrl) throws IOException {
        File pathFile = new File(path);
        if (!pathFile.exists()) {
            pathFile.mkdirs();
        }

        URL url = new URL(fileUrl);
        InputStream inStream = url.openConnection().getInputStream();
        FileOutputStream fs = new FileOutputStream(path + "/" + fileName);

        byte[] buffer = new byte[1204];
        int byteread = 0;
        while ((byteread = inStream.read(buffer)) != -1) {
            fs.write(buffer, 0, byteread);
        }
        fs.close();
    }
}
