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
import org.elasticsearch.node.Node;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Class to represent a ElasticConnector Connection.
 *
 * @author $author$
 * @version $Revision$ $Date$
 */
public class ElasticConnection {

    private final Client client;

    /**
     * Constructor of ElasticConnectorConnection class.
     *
     * @param configuration the actual {@link ElasticConfiguration}
     */
    public ElasticConnection(ElasticConfiguration configuration) {
        final Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", configuration.getClusterName()).build();
        final TransportClient transportClient = new TransportClient(settings);
        for(String host : configuration.getHosts()) {
            final String[] parts = host.split(":");
            final String server = parts[0];
            final int port = parts.length > 1 ? Integer.valueOf(parts[1]) : 9300;
            transportClient.addTransportAddress(new InetSocketTransportAddress(server, port));
        }
        this.client = transportClient;
    }

    /**
     * Release internal resources.
     */
    public void dispose() {
        client.close();
    }

    public Client client() {
        return client;
    }
}
