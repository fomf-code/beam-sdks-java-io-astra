package org.apache.beam.sdk.io.astra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.dtsx.astra.sdk.AstraOpsClient;
import com.dtsx.astra.sdk.db.AstraDBOpsClient;
import com.dtsx.astra.sdk.db.DbOpsClient;
import com.dtsx.astra.sdk.db.domain.DatabaseCreationBuilder;
import com.dtsx.astra.sdk.db.domain.DatabaseCreationRequest;
import com.dtsx.astra.sdk.db.domain.DatabaseStatusType;
import com.dtsx.astra.sdk.utils.AstraRc;
import com.dtsx.astra.sdk.utils.Utils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * To run unit test you need to set the following environment variables:
 * - ASTRA_DB_APPLICATION_TOKEN
 */
public abstract class AbstractAstraTest {

    /** Logger. */
    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractAstraTest.class);

    /** Test Constants. */
    public static final String TEST_REGION = "us-east1";

    /**
     * Hold reference to token
     */
    protected static String token;

    /**
     * Working db.
     */
    protected static DbOpsClient dbOpsClient;

    /**
     * Reference to Databases Client.
     */
    private static AstraOpsClient astraOpsClient;

    /**
     * Reference to organization client.
     */
    protected static AstraDBOpsClient astraDbOpsClient;

    /**
     * Reference to session.
     */
    protected static CqlSession cqlSession;

    /**
     * Access DB client.
     *
     * @return
     *      client fot databases
     */
    protected static AstraOpsClient getAstraOpsClient() {
        if (astraOpsClient == null) {
            astraOpsClient = new AstraOpsClient(getToken());
        }
        return astraOpsClient;
    }

    /**
     * Access DB client.
     *
     * @return
     *      client fot databases
     */
    protected static AstraDBOpsClient getAstraDbOpsClient() {
        if (astraDbOpsClient == null) {
            astraDbOpsClient = new AstraDBOpsClient(getToken());
        }
        return astraDbOpsClient;
    }

    protected static String getSecureConnectBundleFilePath(String dbName) {
        return "/tmp/scb-"+ dbName + ".zip";
    }

    /**
     * Read secure connect bundle path for tests.
     *
     * @return
     *      token for test or error
     */
    protected static Path getSecureConnectBundlePath(String dbName, String keyspace) {
        File secureConnectBundle = new File(getSecureConnectBundleFilePath(dbName));
        if (!new File(getSecureConnectBundleFilePath(dbName)).exists()) {
            LOGGER.info("Downloading SCB  {} as not existing ", getSecureConnectBundleFilePath(dbName));
            try {
                createDbAndProvideClient(dbName, keyspace, false).
                        downloadDefaultSecureConnectBundle(getSecureConnectBundleFilePath(dbName));
            } catch (InterruptedException e) {
               throw new IllegalStateException(e);
            }
        }
        return Path.of(getSecureConnectBundleFilePath(dbName));
    }

    protected static byte[] getSecureBundle(String dbname, String keyspace) {
        try {
            return Files.readAllBytes(getSecureConnectBundlePath(dbname, keyspace));
        } catch (IOException e) {
            throw new IllegalArgumentException("cannot read SCB file", e);
        }
    }

    /**
     * Initialization of the cluster.
     *
     * @return
     *      Cassandra cluster
     */
    protected static CqlSession getCqlSession(String dbName, String keyspace) {
        System.out.println(getSecureConnectBundlePath(dbName, keyspace));
        if (cqlSession == null || cqlSession.isClosed()) {
            cqlSession = CqlSession.builder()
                    .withCloudSecureConnectBundle(getSecureConnectBundlePath(dbName, keyspace))
                    .withAuthCredentials("token", getToken())
                    .withKeyspace(keyspace)
                    .build();
        }
        return cqlSession;
    }

    /**
     * Read Token for tests.
     *
     * @return
     *      token for test or error
     */
    protected static String getToken() {
        if (token == null) {
            if (AstraRc.isDefaultConfigFileExists()) {
                token = new AstraRc()
                        .getSectionKey(AstraRc.ASTRARC_DEFAULT, AstraRc.ASTRA_DB_APPLICATION_TOKEN)
                        .orElse(null);
            }
            token = Utils.readEnvVariable(AstraRc.ASTRA_DB_APPLICATION_TOKEN).orElse(token);
        }
        if (token ==null) {
            throw new IllegalStateException("Create environment variable " +
                    "ASTRA_DB_APPLICATION_TOKEN with your token");
        }
        return token;
    }

    /**
     * Create DB if not exist
     *
     * @return
     *      database client
     */
    protected static DbOpsClient createDbAndProvideClient(String dbName, String keyspace, boolean vector)
    throws InterruptedException {
        if (dbOpsClient == null) {
            if (!getAstraDbOpsClient().databaseByName(dbName).find().isPresent()) {
                LOGGER.info("Create DB  {} as not existing ", dbName);
                DatabaseCreationBuilder dbcb = DatabaseCreationRequest
                        .builder()
                        .name(dbName)
                        .keyspace(keyspace)
                        .cloudRegion(TEST_REGION);
                if (vector) {
                    dbcb.withVector();
                }
                getAstraDbOpsClient().create(dbcb.build());
            }

            dbOpsClient = getAstraDbOpsClient().databaseByName(dbName);
            Assert.assertTrue(dbOpsClient.exist());
        }
        while(dbOpsClient.get().getStatus() != DatabaseStatusType.ACTIVE) {
            LOGGER.info("+ Waiting for the db to become ACTIVE ");
            Thread.sleep(5000);
        }
        if (!dbOpsClient.keyspaces().findAll().contains(keyspace)) {
           dbOpsClient.keyspaces().create(keyspace);
        }
        while(dbOpsClient.get().getStatus() != DatabaseStatusType.ACTIVE) {
            LOGGER.info("+ Waiting for the db to become ACTIVE ");
            Thread.sleep(5000);
        }
        return dbOpsClient;
    }

}
