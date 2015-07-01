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

import org.identityconnectors.common.StringUtil;
import org.identityconnectors.framework.spi.AbstractConfiguration;
import org.identityconnectors.framework.spi.ConfigurationProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;


/**
 * Extends the {@link AbstractConfiguration} class to provide all the necessary
 * parameters to initialize the ElasticConnector Connector.
 *
 * @author $author$
 * @version $Revision$ $Date$
 */
public class ElasticConfiguration extends AbstractConfiguration {


    // Exposed configuration properties.

    /**
     * The connector to connect to.
     */
    private String[] hosts;

    /**
     * The index to use.
     */
    private String index;

    /**
     * The index to use.
     */
    private String type;

    /**
     * The cluster name to connect to.
     */
    private String clusterName = "elasticsearch";


    /**
     * Constructor.
     */
    public ElasticConfiguration() {
        this.hosts = new String[0];
    }


    @ConfigurationProperty(order = 1, displayMessageKey = "hosts.display",
            groupMessageKey = "basic.group", helpMessageKey = "hosts.help",
            required = true, confidential = false)
    public String[] getHosts() {
        return hosts;
    }

    public void setHosts(String... hosts) {
        withHosts(hosts);
    }

    public ElasticConfiguration withHosts(String... hosts) {
        if (hosts != null) {
            this.hosts = hosts;
        }
        return this;
    }

    @ConfigurationProperty(order = 2, displayMessageKey = "index.display",
            groupMessageKey = "basic.group", helpMessageKey = "index.help",
            required = true, confidential = false)
    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        withIndex(index);
    }

    public ElasticConfiguration withIndex(String index) {
        this.index = index;
        return this;
    }

    @ConfigurationProperty(order = 3, displayMessageKey = "type.display",
            groupMessageKey = "basic.group", helpMessageKey = "type.help",
            required = true, confidential = false)
    public String getType() {
        return type;
    }

    public void setType(String type) {
        withType(type);
    }

    public ElasticConfiguration withType(String type) {
        this.type = type;
        return this;
    }

    @ConfigurationProperty(order = 4, displayMessageKey = "clusterName.display",
            groupMessageKey = "basic.group", helpMessageKey = "clusterName.help",
            required = true, confidential = false)
    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        withClusterName(clusterName);
    }

    public ElasticConfiguration withClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public void validate() {
        if (hosts.length == 0) {
            throw new IllegalArgumentException("Hosts cannot be null or empty.");
        }
        if (StringUtil.isBlank(index)) {
            throw new IllegalArgumentException("Index cannot be null or empty.");
        }
        if (StringUtil.isBlank(type)) {
            throw new IllegalArgumentException("Type cannot be null or empty.");
        }
    }

}
