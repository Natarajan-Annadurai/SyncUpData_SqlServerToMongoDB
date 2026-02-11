package org.example;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.*;

public class DailyScheduler {

    public static void main(String[] args) {

        ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor();

        Runnable task = () -> {
            System.out.println("Starting Daily Sync...");
            new SyncJob().run();
        };

        long initialDelay = computeInitialDelay();
        long period = TimeUnit.DAYS.toSeconds(1);

        scheduler.scheduleAtFixedRate(task, initialDelay, period, TimeUnit.SECONDS);
    }

    private static long computeInitialDelay() {

        LocalDateTime now = LocalDateTime.now();
        LocalDateTime nextRun =
                now.withHour(6).withMinute(0).withSecond(0);

        if (now.compareTo(nextRun) > 0) {
            nextRun = nextRun.plusDays(1);
        }

        return Duration.between(now, nextRun).getSeconds();
    }
}