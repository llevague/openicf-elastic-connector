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


import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.api.APIConfiguration;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.api.ConnectorFacadeFactory;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.Filter;
import org.identityconnectors.framework.common.objects.filter.FilterBuilder;
import org.identityconnectors.test.common.PropertyBag;
import org.identityconnectors.test.common.TestHelpers;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Attempts to test the {@link ElasticConnector} with the framework.
 *
 * @author $author$
 * @version $Revision$ $Date$
 */
public class ElasticConnectorTests {



    //set up logging
    private static final Log LOGGER = Log.getLog(ElasticConnectorTests.class);

    private static final PropertyBag PROPERTIES = TestHelpers.getProperties(ElasticConnector.class);
    // Host is a public property read from public configuration file
    private ConnectorFacade facade;

    private ObjectClass objectClass;

    @BeforeClass
    @SuppressWarnings("unchecked")
    public void setUp() {
        ElasticConfiguration config = new ElasticConfiguration()
                .withHosts(PROPERTIES.getProperty("configuration.hosts", String[].class))
                .withIndex(PROPERTIES.getStringProperty("configuration.index"))
                .withType(PROPERTIES.getStringProperty("configuration.type"))
                .withClusterName(PROPERTIES.getStringProperty("configuration.clusterName"));
        this.facade = getFacade(config);
        this.objectClass = new ObjectClass("MYUSER");
    }

    @AfterClass
    public void tearDown() {
        facade.delete(objectClass, new Uid("123456789"), new OperationOptionsBuilder().build());
    }

    @Test(enabled = true)
    public void create() {
        LOGGER.info("Running Test 1...");
        //You can use TestHelpers to do some of the boilerplate work in running a search
        //TestHelpers.search(theConnector, ObjectClass.ACCOUNT, filter, handler, null);

        @SuppressWarnings("serial")
        final Set<Attribute> attrs = new HashSet<Attribute>() {{
            add(AttributeBuilder.build("__UID__","123456789"));
            add(AttributeBuilder.build("__NAME__","DOE"));
            add(AttributeBuilder.build("ID","123456789"));
            add(AttributeBuilder.build("NAME", "DOE"));
            add(AttributeBuilder.build("FIRSTNAME","John"));
            add(AttributeBuilder.build("BIRTHDATE", "1979-06-02"));
            add(AttributeBuilder.build("PROFILE", "ADMIN", "GESTIONNAIRE"));
        }};

        final Uid uid = facade.create(objectClass, attrs, new OperationOptionsBuilder().build());
        LOGGER.warn("Uid=" + uid.getUidValue());
    }

    @Test(dependsOnMethods = { "create" })
    public void read() {
        System.out.println("read method call =======================");
        final Filter filter = FilterBuilder.equalTo(AttributeBuilder.build("FIRSTNAME", "John"));
        final List<ConnectorObject> results = TestHelpers.searchToList(facade,
                objectClass,
                filter,
                new OperationOptionsBuilder().build());
        System.out.println("Search results =======================");
        for (ConnectorObject result : results) {
            System.out.println(result.getAttributeByName("ID"));
        }
    }

    protected ConnectorFacade getFacade(ElasticConfiguration config) {
        ConnectorFacadeFactory factory = ConnectorFacadeFactory.getInstance();
        // **test only**
        APIConfiguration impl = TestHelpers.createTestConfiguration(ElasticConnector.class, config);
        return factory.newInstance(impl);
    }
}
