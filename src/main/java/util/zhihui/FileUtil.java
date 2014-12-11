package util.zhihui;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

public class FileUtil {
    private FileUtil() { }

    public static void writeToFile(String fileName, String content) {
        Writer writer = null;
        try {
            writer = new FileWriter(fileName, true);
            writer.write(content);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                }
            }
        }
    }
}
