/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.frameworkset.tran.mongodb.cdc;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClientSettings;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;



/**
 * A change stream aggregation pipeline, used to modify the output of a MongoDB change stream.
 *
 * @see <a href="https://www.mongodb.com/docs/manual/changeStreams/#modify-change-stream-output.">Modify Change Stream Output</a>
 */
public class ChangeStreamPipeline {

    private List<? extends Bson> stages;

    public ChangeStreamPipeline(String json) {
        this.stages = parse(json);
    }

    public ChangeStreamPipeline(List<? extends Bson> stages) {
        this.stages = stages;
    }

    public ChangeStreamPipeline(Bson... stages) {
        this(Arrays.asList(stages));
    }

    public List<? extends Bson> getStages() {
        return stages;
    }

    /**
     * Creates a new pipeline that is a combination of the current and supplied pipeline stages in serial.
     *
     * @param pipeline the pipeline to add in serial.
     * @return the combined pipeline
     */
    public ChangeStreamPipeline then(ChangeStreamPipeline pipeline) {
        List<Bson> stages = new ArrayList<Bson>();
        stages.addAll(this.getStages());
        stages.addAll(pipeline.getStages());
        return new ChangeStreamPipeline(stages);
    }

    public String toString() {
        return format(stages);
    }

    private static String format(List<? extends Bson> stages) {
        return new BasicDBObject("stages", stages)
                .toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry())
                .getArray("stages")
                .getValues()
                .toString();
    }

    private static List<? extends Bson> parse(String json) {
        if (json == null || json.isEmpty()) {
            return Collections.emptyList();
        }

        // Top-level for `parse` must be a document not a list, hence this trick
        return Document.parse("{stages: " + json + "}")
                .getList("stages", Document.class);
    }

}
