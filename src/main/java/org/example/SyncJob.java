package org.example;

public class SyncJob {

    public void run() {
        try {
            // Step 1: Export CSV
            ExportQueryToCSV obj = new ExportQueryToCSV();
            obj.generateCSVFile();

            // Step 2: Update Mongo
            SqlToMongoUpdate.process();

            JobLogger.success("DAILY_SYNC", "Full sync completed successfully");

        } catch (Exception e) {
            JobLogger.failure("DAILY_SYNC", e);
        }
    }
}
