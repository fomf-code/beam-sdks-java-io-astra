package org.apache.beam.sdk.io.astra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.dtsx.astra.sdk.AstraDevopsApiClient;
import com.dtsx.astra.sdk.db.AstraDbClient;
import com.dtsx.astra.sdk.db.DatabaseClient;
import com.dtsx.astra.sdk.db.domain.DatabaseCreationRequest;
import com.dtsx.astra.sdk.utils.AstraRc;
import com.dtsx.astra.sdk.utils.Utils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * To run unit test you need to set the following environment variables:
 * - ASTRA_DB_APPLICATION_TOKEN
 */
public abstract class AbstractAstraDbTest {

    /** Logger. */
    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractAstraDbTest.class);

    /** Test Constants. */
    public static final String TEST_DB_NAME = "beam_sdk_integration_test";

    /** Test Constants. */
    public static final String TEST_KEYSPACE = "beam";

    /** Test Constants. */
    public static final String TEST_REGION = "us-east1";

    /** Test Constants. */
    protected static final String TEST_SCB_PATH = "/tmp/scb-"+ TEST_DB_NAME + ".zip";

    /**
     * Hold reference to token
     */
    protected static String token;

    /**
     * Hold reference to Secure Connect Bundle path.
     */
    protected static String secureConnectBundlePath = TEST_SCB_PATH;

    /**
     * Working db.
     */
    protected static DatabaseClient dbClient;

    /**
     * Reference to Databases Client.
     */
    private static AstraDbClient databasesClient;

    /**
     * Reference to organization client.
     */
    protected static AstraDevopsApiClient apiDevopsClient;

    /**
     * Reference to cluster.
     */
    protected static Cluster cluster;

    /**
     * Reference to session.
     */
    protected static Session session;

    /**
     * Read secure connect bundle path for tests.
     *
     * @return
     *      token for test or error
     */
    protected static File getSecureConnectBundleFile() {
        File secureConnectBundle = new File(secureConnectBundlePath);
        if (!new File(secureConnectBundlePath).exists()) {
            LOGGER.info("Downloading SCB  {} as not existing ", secureConnectBundlePath);
            createDbAndProvideClient().downloadDefaultSecureConnectBundle(secureConnectBundlePath);
        }
        return secureConnectBundle;
    }

    /**
     * Initialization of the cluster.
     *
     * @return
     *      Cassandra cluster
     */
    protected static Cluster getCluster() {
        if (cluster == null) {
            cluster = Cluster.builder()
                    .withCloudSecureConnectBundle(getSecureConnectBundleFile())
                    .withCredentials("token", getToken())

                    .build();
        }
        return cluster;
    }

    /**
     * Return a session
     * @return
     */
    protected static Session getSession() {
        if (session == null) {
            LOGGER.info("Initializing Session with keyspace {}", TEST_KEYSPACE);
            session = getCluster().connect(TEST_KEYSPACE);
        }
        return session;
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
     * Access DB client.
     *
     * @return
     *      client fot databases
     */
    protected static AstraDbClient getDatabasesClient() {
        if (databasesClient == null) {
            databasesClient = new AstraDbClient(getToken());
        }
        return databasesClient;
    }

    /**
     * Access DB client.
     *
     * @return
     *      client fot databases
     */
    protected static AstraDevopsApiClient getApiDevopsClient() {
        if (apiDevopsClient == null) {
            apiDevopsClient = new AstraDevopsApiClient(getToken());
        }
        return apiDevopsClient;
    }

    /**
     * Create DB if not exist
     *
     * @return
     *      database client
     */
    protected static DatabaseClient createDbAndProvideClient() {
        if (dbClient == null) {
            if (!getDatabasesClient().findByName(TEST_DB_NAME).findAny().isPresent()) {
                LOGGER.info("Create DB  {} as not existing ", TEST_DB_NAME);
                getDatabasesClient().create(DatabaseCreationRequest
                        .builder()
                        .name(TEST_DB_NAME)
                        .keyspace(TEST_KEYSPACE)
                        .cloudRegion(TEST_REGION)
                        .build());
            }
            dbClient = getApiDevopsClient().db().databaseByName(TEST_DB_NAME);
            Assert.assertTrue(dbClient.exist());
        }
        return dbClient;
    }

    protected static final void cleanup() {
        if (session!= null) {
            session.close();
        }
        if (cluster!= null) {
            cluster.close();
        }
    }

}
