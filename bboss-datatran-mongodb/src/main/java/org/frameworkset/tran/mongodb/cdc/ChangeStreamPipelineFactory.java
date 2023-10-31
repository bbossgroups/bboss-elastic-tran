/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.frameworkset.tran.mongodb.cdc;

import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.OperationType;
import org.bson.conversions.Bson;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.plugin.mongocdc.MongoCDCInputConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * A factory to produce a MongoDB change stream pipeline expression.
 */
public class ChangeStreamPipelineFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeStreamPipelineFactory.class);

    private final MongoCDCInputConfig connectorConfig;

    public ChangeStreamPipelineFactory(MongoCDCInputConfig connectorConfig) {
        this.connectorConfig = connectorConfig;

    }

    public ChangeStreamPipeline create() {
        // Resolve and combine internal and user pipelines serially
        ChangeStreamPipeline internalPipeline = createInternalPipeline();
        ChangeStreamPipeline userPipeline = createUserPipeline();
        if(userPipeline != null && internalPipeline != null) {
            ChangeStreamPipeline effectivePipeline = internalPipeline.then(userPipeline);

            LOGGER.info("Effective change stream pipeline: {}", effectivePipeline);
            return effectivePipeline;
        }
        else{
            if(internalPipeline != null)
                return internalPipeline;

            if(userPipeline != null)
                return userPipeline;

        }
        return null;
    }

    private ChangeStreamPipeline createInternalPipeline() {
        // Resolve the leaf filters
        Bson collectionFilter = createCollectionFilter( );
        Bson operationTypeFilter = createOperationTypeFilter( );


        // Combine
        Bson andFilter = Filters.and(collectionFilter,operationTypeFilter);
        Bson matchFilter = Aggregates.match(andFilter);

        // Pipeline
        // Note that change streams cannot use indexes:
        // - https://www.mongodb.com/docs/manual/administration/change-streams-production-recommendations/#indexes-and-performance
        // Note that `$addFields` must be used over `$set`/ `$unset` to support MongoDB 4.0 which doesn't support these operators:
        // - https://www.mongodb.com/docs/manual/changeStreams/#modify-change-stream-output
        Map<String,Object> params = new LinkedHashMap();
        params.put("namespace",concat("$ns.db", ".", "$ns.coll"));
        params.put("event", "$$ROOT");
        return new ChangeStreamPipeline(
                // Materialize a "namespace" field so that we can do qualified collection name matching per
                // the configuration requirements
                // We can't use $addFields nor $set as there is no way to unset the filed for AWS DocumentDB
                // Note that per the docs, if `$ns` doesn't exist, `$concat` will return `null`

                Aggregates.replaceRoot(new BasicDBObject(params)),
                // Filter the documents
                matchFilter,

                // This is required to prevent driver `ChangeStreamDocument` deserialization issues:
                Aggregates.replaceRoot("$event"));
    }

    private ChangeStreamPipeline createUserPipeline() {
        // Delegate to the configuration
        return this.connectorConfig.getChangeStreamPipeline();
    }

    private   Bson createCollectionFilter( ) {
        // Database filters
        // Note: No need to exclude `filterConfig.getBuiltInDbNames()` since these are not streamed per
        // https://www.mongodb.com/docs/manual/changeStreams/#watch-a-collection--database--or-deployment
        Bson dbFilters = null;
        if (this.connectorConfig.getDbIncludeList() != null) {
            dbFilters = Filters.regex("event.ns.db", connectorConfig.getDbIncludeList().replaceAll(",", "|"), "i");
        }
        else if (connectorConfig.getDbExcludeList() != null) {
            dbFilters = Filters.regex("event.ns.db", "(?!" + connectorConfig.getDbExcludeList().replaceAll(",", "|") + ")", "i");
        }

        // Collection filters
        Bson collectionsFilters = null;
        if (connectorConfig.getCollectionIncludeList() != null) {
            collectionsFilters = Filters.regex("namespace", connectorConfig.getCollectionIncludeList().replaceAll(",", "|"), "i");
        }
        else if (connectorConfig.getCollectionExcludeList() != null) {
            collectionsFilters = Filters.regex("namespace", "(?!" + connectorConfig.getCollectionExcludeList().replaceAll(",", "|") + ")", "i");
        }
        Bson includedSignalCollectionFilters = null;
        if (connectorConfig.getSignalDataCollection() != null) {
            includedSignalCollectionFilters = Filters.eq("namespace", connectorConfig.getSignalDataCollection());
        }

        // Combined filters
        return andFilters(
                dbFilters,
                orFilters(
                        includedSignalCollectionFilters,
                        collectionsFilters));
    }

    private Bson createOperationTypeFilter() {
        // Per https://debezium.io/documentation/reference/stable/connectors/mongodb.html#mongodb-property-skipped-operations
        // > The supported operations include:
        // > - 'c' for inserts/create
        // > - 'u' for updates/replace,
        // > - 'd' for deletes,
        // > - 't' for truncates, and
        // > - 'none' to not skip any operations.
        // > By default, 'truncate' operations are skipped (not emitted by this connector).
        // However, 'truncate' is not supported since it doesn't exist as a
        // [MongoDB change type](https://www.mongodb.com/docs/manual/reference/change-events/). Also note that
        // support for 'none' effectively implies 'c', 'u', 'd'

        // First, begin by including all the supported Debezium change events
        List<String> includedOperations = new ArrayList<>();


        // Next, remove any implied by the configuration
        List<Integer> includedOperations_ = this.connectorConfig.getIncludedOperations();
        for(int i = 0; includedOperations_ != null && i < includedOperations_.size(); i ++) {
            int op = includedOperations_.get(i);
            if (op == Record.RECORD_INSERT) {
                includedOperations.add(OperationType.INSERT.getValue());
            }
            if (op == Record.RECORD_UPDATE) {
                includedOperations.add(OperationType.UPDATE.getValue());
                includedOperations.add(OperationType.REPLACE.getValue());
            }
            if (op == Record.RECORD_DELETE) {
                includedOperations.add(OperationType.DELETE.getValue());
            }
        }
        if(includedOperations.size() > 0) {
            return Filters.in("event.operationType", includedOperations);
        }
        else{
            return null;
        }
    }

    @SafeVarargs
    private static Bson andFilters(Bson... filters) {
        List<Bson> resolved = resolveFilters(filters);
        if (resolved.isEmpty()) {
            return null;
        }
        else if (resolved.size() == 1) {
            return resolved.get(0);
        }
        else {
            return Filters.and(resolved);
        }
    }

    @SafeVarargs
    private static Bson orFilters(Bson... filters) {
        List<Bson> resolved = resolveFilters(filters);
        if (resolved.isEmpty()) {
            return null;
        }
        else if (resolved.size() == 1) {
            return resolved.get(0);
        }
        else {
            return Filters.or(resolved);
        }
    }

    @SafeVarargs
    private static List<Bson> resolveFilters(Bson... filters) {
        if(filters == null || filters.length == 0)
            return new ArrayList<>();
        return Arrays.asList(filters);

//        return Stream.of(filters)
//                .flatMap(Optional::stream)
//                .collect(toList());
    }

    private static Bson concat(Object... expressions) {
        return new BasicDBObject("$concat", Arrays.asList(expressions));
    }
}
