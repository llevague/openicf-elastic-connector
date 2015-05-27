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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.Connector;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.operations.*;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

import static org.elasticsearch.index.query.FilterBuilders.orFilter;
import static org.elasticsearch.index.query.FilterBuilders.rangeFilter;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;

/**
 * Main implementation of the ElasticConnector Connector.
 *
 * @author $author$
 * @version $Revision$ $Date$
 */
@ConnectorClass(
        displayNameKey = "ElasticConnector.connector.display",
        configurationClass = ElasticConfiguration.class)
public class ElasticConnector implements
        Connector
        , CreateOp
        , DeleteOp
        , UpdateOp
        , SearchOp<FilterBuilder>
        , SyncOp
{
    /**
     * Setup logging for the {@link ElasticConnector}.
     */
    private static final Log logger = Log.getLog(ElasticConnector.class);

    /**
     * Place holder for the Connection created in the init method.
     */
    private ElasticConnection connection;

    /**
     * Place holder for the {@link Configuration} passed into the init() method
     * {@link ElasticConnector#init(org.identityconnectors.framework.spi.Configuration)}.
     */
    private ElasticConfiguration configuration;

    /**
     * Gets the Configuration context for this connector.
     *
     * @return The current {@link Configuration}
     */
    public Configuration getConfiguration() {
        return this.configuration;
    }

    /**
     * Callback method to receive the {@link Configuration}.
     *
     * @param configuration the new {@link Configuration}
     * @see org.identityconnectors.framework.spi.Connector#init(org.identityconnectors.framework.spi.Configuration)
     */
    public void init(final Configuration configuration) {
        this.configuration = (ElasticConfiguration) configuration;
        this.connection = new ElasticConnection(this.configuration);
    }

    /**
     * Disposes of the {@link ElasticConnector}'s resources.
     *
     * @see org.identityconnectors.framework.spi.Connector#dispose()
     */
    public void dispose() {
        configuration = null;
        if (connection != null) {
            connection.dispose();
            connection = null;
        }
    }


    /******************
     * SPI Operations
     *
     * Implement the following operations using the contract and
     * description found in the Javadoc for these methods.
     ******************/



    /**
     * {@inheritDoc}
     */
    @Override
    public Uid create(final ObjectClass objectClass, final Set<Attribute> createAttributes,
                      final OperationOptions options) {
        final Uid uid = AttributeUtil.getUidAttribute(createAttributes);
        connection.client()
                .prepareIndex(configuration.getIndex(), objectClass.getObjectClassValue())
                .setId(uid.getUidValue())
                .setSource(attributesToBytes(createAttributes))
                .get();
        return uid;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(final ObjectClass objectClass, final Uid uid,
                       final OperationOptions options) {
        connection.client()
                .prepareDelete(configuration.getIndex(), objectClass.getObjectClassValue(), uid.getUidValue())
                .get();

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Uid update(ObjectClass objectClass, Uid uid, Set<Attribute> replaceAttributes,
                      OperationOptions options) {
        connection.client()
                .prepareUpdate(configuration.getIndex(), objectClass.getObjectClassValue(), uid.getUidValue())
                .setDoc(attributesToBytes(replaceAttributes))
                .get();
        return uid;
    }

    @Override
    public FilterTranslator<FilterBuilder> createFilterTranslator(ObjectClass oclass, OperationOptions options) {
        return new ElasticFilterTranslator();
    }

    @Override
    public void executeQuery(ObjectClass oclass, FilterBuilder filter, ResultsHandler handler, OperationOptions options) {
        final List<ConnectorObject> searchResults = doSearch(oclass, filter, options);
        for (ConnectorObject connectorObject : searchResults) {
            handler.handle(connectorObject);
        }
    }

    @Override
    public void sync(final ObjectClass oclass, final SyncToken token,
                     final SyncResultsHandler handler, final OperationOptions options) {
        final String now = getNowTime();
        final String creationTimestamp = "_idmCreationTimestamp";
        final String updateTimestamp = "_idmUpdateTimestamp";
        final String value = token.getValue().toString();

        final FilterBuilder filter = orFilter(
                rangeFilter(creationTimestamp).gte(value),
                rangeFilter(updateTimestamp).gte(value));

        final List<ConnectorObject> searchResults = doSearch(oclass, filter, options);
        for (ConnectorObject connectorObject : searchResults) {
            final SyncDeltaBuilder syncDeltaBuilder = new SyncDeltaBuilder();
            syncDeltaBuilder.setToken(new SyncToken(now));
            syncDeltaBuilder.setDeltaType(SyncDeltaType.CREATE_OR_UPDATE);
            syncDeltaBuilder.setUid(connectorObject.getUid());
            syncDeltaBuilder.setObject(connectorObject);

            handler.handle(syncDeltaBuilder.build());
        }
    }

    @Override
    public SyncToken getLatestSyncToken(ObjectClass objClass) {
        return new SyncToken(getNowTime());
    }

    private List<ConnectorObject> doSearch(ObjectClass oclass, FilterBuilder filter, OperationOptions options) {
        final List<ConnectorObject> results = new ArrayList<>();

        final SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(filteredQuery(null, filter));
        final SearchResponse response = connection.client()
                .prepareSearch(configuration.getIndex())
                .setSource(sourceBuilder.buildAsBytes())
                .setExplain(true)
                .get();

        for (SearchHit hit : response.getHits().hits()) {
            final ConnectorObjectBuilder cobld = new ConnectorObjectBuilder();
            cobld.setUid(hit.getId());
            for (Entry<String, Object> entry : hit.getSource().entrySet()) {
                final String attrName = entry.getKey();
                final Object attrValue = entry.getValue();

                if (!attrName.equalsIgnoreCase("_id") && !attrName.equalsIgnoreCase("_rev")) {
                    if (attrValue instanceof Collection) {
                        cobld.addAttribute(AttributeBuilder.build(attrName, (Collection) attrValue));
                    } else if (attrValue != null) {
                        cobld.addAttribute(AttributeBuilder.build(attrName, attrValue));
                    } else {
                        cobld.addAttribute(AttributeBuilder.build(attrName));
                    }
                }
            }
            cobld.setObjectClass(oclass);
            results.add(cobld.build());
        }
        return results;
    }

    private byte[] attributesToBytes(Set<Attribute> attributes) {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            final ObjectNode objectNode = mapper.createObjectNode();
            for (Attribute attr : attributes) {
                    final List<Object> value = attr.getValue();
                    objectNode.putPOJO(attr.getName(),
                            value != null && value.size() > 1 ? value : AttributeUtil.getSingleValue(attr));
                }
            return mapper.writeValueAsBytes(objectNode);
        } catch (JsonProcessingException e) {
            logger.error(e, null);
            return new byte[0];
        }
    }

    private String getNowTime() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        return sdf.format(new Date());
    }
}
