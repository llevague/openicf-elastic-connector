package org.forgerock.openicf.connectors.elastic.ElasticConnector.config

import org.forgerock.openicf.connectors.elastic.ElasticHost

/*
 * DO NOT REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright (c) 2013. ForgeRock Inc. All rights reserved.
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
 *
 */

/* +---------------------------------------------------+
 *  ----------- Contract Tests configuration ------------
 *  +---------------------------------------------------+
 */

import org.identityconnectors.contract.data.groovy.Lazy
import org.identityconnectors.common.security.GuardedString

// Connector WRONG configuration for ValidateApiOpTests
connector.i1.wrong.host=""
connector.i2.wrong.login=""


configuration{
    hosts = ["127.0.0.1:9300"] as String[]
    index = "tests"
}

testsuite {
    // path to bundle jar - property is set by ant - leave it as it is
    bundleJar=System.getProperty("bundleJar")
    bundleName=System.getProperty("bundleName")
    bundleVersion=System.getProperty("bundleVersion")
    connectorName=System.getProperty("connectorName")

    // ValidateApiOpTests:
    Validate.iterations="3"

    // AuthenticationApiOpTests:
    Authentication.__ACCOUNT__.username=Lazy.get("i0.Authentication.__ACCOUNT__.__NAME__")
    Authentication.__ACCOUNT__.wrong.password=new GuardedString("bogus".toCharArray())
} // testsuite

HOST="0.0.0.0"
