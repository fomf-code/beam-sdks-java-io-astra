package org.apache.beam.sdk.io.astra.db.transforms.split;

import com.datastax.oss.driver.internal.core.metadata.SniEndPoint;

import java.io.Serializable;

/**
 * Wrap the SNI EndPoint to be serializable.
 */
public class AstraTokenRangeEndpoint extends SniEndPoint implements Serializable {

    /**
     * Composition constructor.
     *
     * @param sniEndPoint
     *      current SNiEndPoint
     */
    public AstraTokenRangeEndpoint(SniEndPoint sniEndPoint) {
        super(sniEndPoint.resolve(), sniEndPoint.getServerName());
    }
}
