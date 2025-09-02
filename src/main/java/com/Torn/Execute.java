package com.Torn;

import com.Torn.FactionCrimes.Available.GetAvailableCrimes;
import com.Torn.Helpers.Constants;
import com.Torn.Helpers.HttpTriggerServer;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

import static com.Torn.ApiKeys.ValidateApiKeys.Validate;
import static com.Torn.FactionMembers.SyncMembers.syncFactionMembers;

// Execute.java - Simplified for cron jobs
public class Execute {
    private static final Logger logger = LoggerFactory.getLogger(Execute.class);

    public static void main(String[] args) throws SQLException, IOException {
        logger.info("Application starting...");

        // Load PostgreSQL driver
        try {
            Class.forName("org.postgresql.Driver");
            logger.info("PostgreSQL JDBC driver loaded successfully");
        } catch (ClassNotFoundException e) {
            logger.error("PostgreSQL JDBC driver not found in classpath", e);
            System.exit(1);
        }

        String jobCode = getJobCode();
        if (jobCode == null) {
            logger.error("Execute_Job environment variable not set.");
            System.exit(1);
        }

        try {
            switch (jobCode) {
                case Constants.JOB_VALIDATE_API_KEYS:
                    logger.info("Running API key validation job");
                    Validate();
                    break;
                case Constants.JOB_GET_FACTION_MEMBERS:
                    logger.info("Running faction members sync job");
                    syncFactionMembers();
                    break;
                case Constants.JOB_UPDATE_OVERVIEW_DATA:
                    logger.info("Running overview data update job");
                    // Add your implementation
                    break;
                case Constants.JOB_UPDATE_COMPLETED_DATA:
                    logger.info("Running completed data update job");
                    // Add your implementation
                    break;
                case Constants.JOB_CHECK_USER_ITEMS:
                    logger.info("Running user items check job");
                    // Add your implementation
                    break;
                case Constants.JOB_CHECK_AVAILABLE_CRIMES:
                    logger.info("Running available crimes check job");
                    // Add your implementation
                    break;
                default:
                    logger.error("Unknown job code: {}", jobCode);
                    System.exit(1);
            }

            logger.info("Job {} completed successfully", jobCode);
            System.exit(0); // Explicit success exit for cron jobs

        } catch (Exception e) {
            logger.error("Job {} failed", jobCode, e);
            System.exit(1); // Explicit failure exit for cron jobs
        }
    }

    private static String getJobCode() {
        String jobCode = System.getenv("Execute_Job");
        return (jobCode != null && !jobCode.isEmpty()) ? jobCode : null;
    }



//        if (System.getenv(Constants.TORN_LIMITED_API_KEY) == null || System.getenv(Constants.TORN_LIMITED_API_KEY).isEmpty()) {
//            logger.error("TORN_API_KEY environment variable not set");
//            System.exit(1);
//        }
//
//        // Check for immediate run request
//        if (args.length > 0 && "run-now".equals(args[0])) {
//            logger.info("Running immediate data fetch...");
//            GetAvailableCrimes.CrimesPollingJob job = new GetAvailableCrimes.CrimesPollingJob();
//            try {
//                job.execute(null);
//                logger.info("Immediate fetch completed successfully");
//                System.exit(0);
//            } catch (JobExecutionException e) {
//                logger.error("Immediate fetch failed", e);
//                System.exit(1);
//            }
//        }
//
//        try {
//            // Create shared polling job instance
//            GetAvailableCrimes.CrimesPollingJob pollingJobInstance = new GetAvailableCrimes.CrimesPollingJob();
//
//            // Start HTTP server for manual triggers
//            HttpTriggerServer httpServer = new HttpTriggerServer(pollingJobInstance);
//            httpServer.start();
//
//            // Create Quartz scheduler
//            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
//            scheduler.start();
//
//            // Define job
//            JobDetail job = JobBuilder.newJob(GetAvailableCrimes.CrimesPollingJob.class)
//                    .withIdentity("crimesPollingJob", "torn")
//                    .build();
//
//            // Define trigger with configurable interval
//            Trigger trigger = TriggerBuilder.newTrigger()
//                    .withIdentity("crimesTrigger", "torn")
//                    .startNow()
//                    .withSchedule(SimpleScheduleBuilder.simpleSchedule()
//                            .withIntervalInMinutes(POLLING_INTERVAL_MINUTES)
//                            .repeatForever())
//                    .build();
//
//            // Schedule the job
//            scheduler.scheduleJob(job, trigger);
//            logger.info("Torn crimes polling job scheduled successfully. Polling every {} minutes.", POLLING_INTERVAL_MINUTES);
//
//            // Shutdown hook to clean up resources
//            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//                logger.info("Shutting down application...");
//                try {
//                    scheduler.shutdown();
//                    httpServer.stop();
//                } catch (Exception e) {
//                    logger.error("Error during shutdown", e);
//                }
//            }));
//
//            // Keep the application running
//            while (true) {
//                Thread.sleep(60000);
//            }
//
//        } catch (Exception e) {
//            logger.error("Error starting crimes polling job", e);
//            System.exit(1);
//        }
//    }


}
