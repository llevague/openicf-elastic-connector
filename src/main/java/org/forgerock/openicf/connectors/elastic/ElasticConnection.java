/*
 * DO NOT REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright (c) 2013 ForgeRock Inc. All rights reserved.
 *
 * The contents of this file are subject to the terms
 * of the Common Development and Distribution License
 * (the License). You may not use this file except in
 * compliance with the License.
 *
 * You can obtain a copy of the License at
 * http://forgerock.org/license/CDDLv1.0.html
 * See the License for the specific language governing
 * permission and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL
 * Header Notice in each file and include the License file
 * at http://forgerock.org/license/CDDLv1.0.html
 * If applicable, add the following below the CDDL Header,
 * with the fields enclosed by brackets [] replaced by
 * your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 */
package org.forgerock.openicf.connectors.elastic;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.exceptions.ConnectorException;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Class to represent a ElasticConnector Connection.
 *
 * @author $author$
 * @version $Revision$ $Date$
 */
public class ElasticConnection {

    private final static AtomicReference<Client> refClient = new AtomicReference<>();

    /**
     * Constructor of ElasticConnectorConnection class.
     *
     * @param configuration the actual {@link ElasticConfiguration}
     */
    public ElasticConnection(ElasticConfiguration configuration) {
        if (refClient.get() == null) {
            final Settings settings = ImmutableSettings.settingsBuilder()
                    .put("cluster.name", configuration.getClusterName())
                    .build();
            try {
                final TransportClient transportClient = new TransportClient(settings);
                for (String host : configuration.getHosts()) {
                    try {
                        final String[] parts = host.split(":");
                        final String server = parts[0];
                        final int port = parts.length > 1 ? Integer.valueOf(parts[1]) : 9300;
                        transportClient.addTransportAddress(new InetSocketTransportAddress(server, port));
                    } catch (NumberFormatException e) {
                        throw new ConfigurationException("Hosts must have 'server:port' form");
                    }
                }
                refClient.set(transportClient);
            } catch (Throwable e) {
                throw new ConnectorException(e);
            }
        }
    }

    public Client client() {
        return refClient.get();
    }
}
