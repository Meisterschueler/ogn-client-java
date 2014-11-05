/**
 * Copyright (c) 2014 OGN, All Rights Reserved.
 */

package org.ogn.client;

import static org.ogn.client.OgnClientConstants.OGN_CLIENT_DEFAULT_KEEP_ALIVE_INTERVAL_MS;
import static org.ogn.client.OgnClientConstants.OGN_DEFAULT_APP_NAME;
import static org.ogn.client.OgnClientConstants.OGN_DEFAULT_APP_VERSION;
import static org.ogn.client.OgnClientConstants.OGN_DEFAULT_APRS_PORT;
import static org.ogn.client.OgnClientConstants.OGN_DEFAULT_APRS_PORT_FILTERED;
import static org.ogn.client.OgnClientConstants.OGN_DEFAULT_RECONNECTION_TIMEOUT_MS;
import static org.ogn.client.OgnClientConstants.OGN_DEFAULT_SERVER_NAME;
import static org.ogn.client.OgnClientProperties.PROP_OGN_CLIENT_APP_NAME;
import static org.ogn.client.OgnClientProperties.PROP_OGN_CLIENT_APP_VERSION;
import static org.ogn.client.OgnClientProperties.PROP_OGN_CLIENT_IGNORE_AIRCRAFT_BEACONS;
import static org.ogn.client.OgnClientProperties.PROP_OGN_CLIENT_IGNORE_RECEIVER_BEACONS;
import static org.ogn.client.OgnClientProperties.PROP_OGN_CLIENT_KEEP_ALIVE_INTERVAL;
import static org.ogn.client.OgnClientProperties.PROP_OGN_SRV_NAME;
import static org.ogn.client.OgnClientProperties.PROP_OGN_SRV_PORT_FILTERED;
import static org.ogn.client.OgnClientProperties.PROP_OGN_SRV_PORT_UNFILTERED;
import static org.ogn.client.OgnClientProperties.PROP_OGN_SRV_RECONNECTION_TIMEOUT;

import org.ogn.client.aprs.AprsOgnClient;

/**
 * This factory creates instances of OGN client. Several parameters can be tuned through the environment variables.
 * 
 * @author wbuczak
 */
public class OgnClientFactory {

    private static String serverName = System.getProperty(PROP_OGN_SRV_NAME, OGN_DEFAULT_SERVER_NAME);

    private static int port = Integer.getInteger(PROP_OGN_SRV_PORT_UNFILTERED, OGN_DEFAULT_APRS_PORT);
    private static int portFiltered = Integer.getInteger(PROP_OGN_SRV_PORT_FILTERED, OGN_DEFAULT_APRS_PORT_FILTERED);
    private static int reconnectionTimeout = Integer.getInteger(PROP_OGN_SRV_RECONNECTION_TIMEOUT,
            OGN_DEFAULT_RECONNECTION_TIMEOUT_MS);

    private static int keepAliveInterval = Integer.getInteger(PROP_OGN_CLIENT_KEEP_ALIVE_INTERVAL,
            OGN_CLIENT_DEFAULT_KEEP_ALIVE_INTERVAL_MS);

    private static String appName = System.getProperty(PROP_OGN_CLIENT_APP_NAME, OGN_DEFAULT_APP_NAME);
    private static String appVersion = System.getProperty(PROP_OGN_CLIENT_APP_VERSION, OGN_DEFAULT_APP_VERSION);

    private static boolean ignoreReceiverBeacons = System.getProperty(PROP_OGN_CLIENT_IGNORE_RECEIVER_BEACONS) != null;
    private static boolean ignoreAircraftBeacons = System.getProperty(PROP_OGN_CLIENT_IGNORE_AIRCRAFT_BEACONS) != null;

    public static OgnClient createClient() {
        return new AprsOgnClient.Builder().serverName(serverName).aprsPort(port).aprsPortFiltered(portFiltered)
                .reconnectionTimeout(reconnectionTimeout).appName(appName).appVersion(appVersion).keepAlive(keepAliveInterval)
                .ignoreReceiverBeacons(ignoreReceiverBeacons).ignoreAicraftrBeacons(ignoreAircraftBeacons).build();
    }

}