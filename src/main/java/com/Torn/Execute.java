package com.Torn;

import com.Torn.Helpers.Constants;
import com.Torn.Postgres.Postgres;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static com.Torn.ApiKeys.ValidateApiKeys.Validate;
import static com.Torn.FactionCrimes.AllCrimes.GetAllOc2CrimesData.fetchAndProcessAllOC2Crimes;
import static com.Torn.FactionCrimes.Available.GetAvailableCrimes.fetchAndProcessAllAvailableCrimes;
import static com.Torn.FactionCrimes.Completed.GetCompletedData.fetchAndProcessAllCompletedCrimes;
import static com.Torn.FactionCrimes.Completed.GetPaidCrimesData.fetchAndProcessAllPaidCrimes;
import static com.Torn.FactionMembers.GetAvailableMembers.fetchAndProcessAllAvailableMembers;
import static com.Torn.FactionMembers.SyncMembers.syncFactionMembers;
import static com.Torn.FactionMembers.UpdateMemberCPR.updateAllFactionsCPR;

public class Execute {
    private static final Logger logger = LoggerFactory.getLogger(Execute.class);
    public static final Postgres postgres = new Postgres();

    public static void main(String[] args) {
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
            cleanup();
            System.exit(1);
        }

        try {
            switch (jobCode) {
                case Constants.JOB_VALIDATE_API_KEYS:
                    logger.info("Running API key validation job");
                    Validate();
                    break; //DONE
                case Constants.JOB_GET_ALL_OC_CRIMES:
                    logger.info("Running Get All OC Data job");
                    fetchAndProcessAllOC2Crimes();
                    break; //DONE
                case Constants.JOB_GET_FACTION_MEMBERS:
                    logger.info("Running faction members sync job");
                    syncFactionMembers();
                    break; //DONE
                case Constants.JOB_UPDATE_OVERVIEW_DATA:
                    logger.info("Running overview data update job");
                    // Add your implementation
                    break;
                case Constants.JOB_UPDATE_COMPLETED_DATA:
                    logger.info("Running completed data update job");
                    fetchAndProcessAllCompletedCrimes(); //TESTING
                    break;
                case Constants.JOB_CHECK_USER_ITEMS:
                    logger.info("Running user items check job");
                    // Add your implementation
                    break;
                case Constants.JOB_UPDATE_CRIMES_PAID_DATA:
                    logger.info("Running paid crimes data update job");
                    fetchAndProcessAllPaidCrimes();
                    break; //DONE
                case Constants.JOB_UPDATE_UPDATE_CPR_DATA:
                    logger.info("Running paid crimes data update job");
                    updateAllFactionsCPR();
                    break; //DONE
                case Constants.JOB_CHECK_AVAILABLE_CRIMES_MEMBERS:
                    logger.info("Running available crimes check job");
                    fetchAndProcessAllAvailableCrimes();
                    fetchAndProcessAllAvailableMembers();
                    //TODO: Work out who should join what crime
                    break;
                default:
                    logger.error("Unknown job code: {}", jobCode);
                    cleanup();
                    System.exit(1);


            }

            logger.info("Job {} completed successfully", jobCode);
            cleanup(); // Clean up before successful exit
            System.exit(0);

        } catch (Exception e) {
            logger.error("Job {} failed", jobCode, e);
            cleanup(); // Clean up before error exit
            System.exit(1);
        }
    }

    private static String getJobCode() {
        String jobCode = System.getenv("Execute_Job");
        return (jobCode != null && !jobCode.isEmpty()) ? jobCode : null;
    }

    /**
     * Clean up resources before application exit
     */
    private static void cleanup() {
        try {
            logger.info("Cleaning up resources...");
            Postgres.cleanup(); // Close all database connection pools
        } catch (Exception e) {
            logger.warn("Error during cleanup: {}", e.getMessage());
        }
    }

}
