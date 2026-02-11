package org.example;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class JobLogger {

    private static final String LOG_FILE = "D:/MahilMartLogs/daily-sync.log";
    private static final DateTimeFormatter FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void success(String jobName, String message) {
        write("SUCCESS", jobName, message, null);
    }

    public static void failure(String jobName, Exception e) {
        write("FAILURE", jobName, e.getMessage(), e);
    }

    private static void write(String status, String jobName, String msg, Exception e) {
        try (FileWriter fw = new FileWriter(LOG_FILE, true)) {
            fw.write("[" + FORMAT.format(LocalDateTime.now()) + "] ");
            fw.write("[" + jobName + "] ");
            fw.write("[" + status + "] ");
            fw.write(msg + "\n");

            if (e != null) {
                fw.write("StackTrace: " + e.toString() + "\n");
            }
            fw.write("-------------------------------------------------\n");
        } catch (IOException io) {
            io.printStackTrace();
        }
    }
}
