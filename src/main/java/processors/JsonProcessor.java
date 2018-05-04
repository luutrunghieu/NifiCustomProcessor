package processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import java.io.IOException;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.ValidationResult.Builder;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.mongodb.AbstractMongoProcessor;
import org.apache.nifi.processors.mongodb.ObjectIdSerializer;

import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.json.CDL;
import org.json.JSONArray;
import org.json.JSONObject;


@Tags({"mongodb", "read", "get", "cdp"})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Get mongo v2 cdp, result CSV or Json Array")
public class JsonProcessor extends AbstractMongoProcessor {
    public static final Validator DOCUMENT_VALIDATOR = (subject, value, context) -> {
        Builder builder = new Builder();
        builder.subject(subject).input(value);
        if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
            return builder.valid(true).explanation("Contains Expression Language").build();
        } else {
            String reason = null;

            try {
                Document.parse(value);
            } catch (RuntimeException var7) {
                reason = var7.getLocalizedMessage();
            }

            return builder.explanation(reason).valid(reason == null).build();
        }
    };
    static final Relationship REL_SUCCESS = (new org.apache.nifi.processor.Relationship.Builder()).name("success").description("All files are routed to success").build();
    static final PropertyDescriptor QUERY;

    static final PropertyDescriptor PROJECTION;
    static final PropertyDescriptor SORT;
    static final PropertyDescriptor LIMIT;
    static final PropertyDescriptor BATCH_SIZE;
    static final PropertyDescriptor RESULTS_PER_FLOWFILE;

    static final String JSON_TYPE_EXTENDED = "Extended";
    static final String JSON_TYPE_STANDARD = "Standard";
    static final AllowableValue JSON_EXTENDED;
    static final AllowableValue JSON_STANDARD;
    static final PropertyDescriptor JSON_TYPE;

    //TruongLX
    static final String CSV_TYPE_RESULT = "text/csv";
    static final String JSON_TYPE_RESULT = "application/json";
    static final AllowableValue CSV_TYPE_RS;
    static final AllowableValue JSON_TYPE_RS;
    static final DateFormat FORMAT_DATE = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");


    static final PropertyDescriptor TYPE_RESULT_INCREMENTAL;
    static final PropertyDescriptor FROM_DATE;
    static final PropertyDescriptor TO_DATE;
    static final PropertyDescriptor RANGE_INCREMENTAL;
    static final PropertyDescriptor QUERY_INCREMENTAL;

    //========================
    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;
    private ObjectMapper mapper;

    public JsonProcessor() {
    }

    public Set<Relationship> getRelationships() {
        return relationships;
    }

    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    private String buildBatch(List<Document> documents, String jsonTypeSetting) throws IOException {
        StringBuilder builder = new StringBuilder();

        for (int index = 0; index < documents.size(); ++index) {
            Document document = (Document) documents.get(index);
            String asJson;
            if (jsonTypeSetting.equals("Standard")) {
                asJson = this.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(document);
            } else {
                asJson = document.toJson(new JsonWriterSettings(true));
            }

            builder.append(asJson).append(documents.size() > 1 && index + 1 < documents.size() ? ", " : "");
        }

        return "[" + builder.toString() + "]";
    }

    private String buildBatchCsvIncremental(List<Document> documents) throws Exception {
        JSONArray jsonArray = new JSONArray();
        for (int index = 0; index < documents.size(); ++index) {
            Document document = (Document) documents.get(index);
            String asJson = this.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(document);
            JSONObject jsonObject = new JSONObject(asJson);
            jsonArray.put(jsonObject);
        }
        return CDL.toString(jsonArray);
    }

    private void configureMapper(String setting) {
        this.mapper = new ObjectMapper();
        if (setting.equals("Standard")) {
            this.mapper.registerModule(ObjectIdSerializer.getModule());
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            this.mapper.setDateFormat(df);
            this.mapper.setTimeZone(TimeZone.getTimeZone("UTC"));
            this.mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        }
    }

    private void writeBatch(final String payload, ProcessContext context, ProcessSession session) {
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            public void process(OutputStream out) throws IOException {
                out.write(payload.getBytes("UTF-8"));
            }
        });
        flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
        session.getProvenanceReporter().receive(flowFile, this.getURI(context));
        session.transfer(flowFile, REL_SUCCESS);
    }

    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        ComponentLog logger = this.getLogger();
        Document query = context.getProperty(QUERY).isSet() ? Document.parse(context.getProperty(QUERY).evaluateAttributeExpressions().getValue()) : null;
        Document projection = context.getProperty(PROJECTION).isSet() ? Document.parse(context.getProperty(PROJECTION).evaluateAttributeExpressions().getValue()) : null;
        Document sort = context.getProperty(SORT).isSet() ? Document.parse(context.getProperty(SORT).evaluateAttributeExpressions().getValue()) : null;
        final String jsonTypeSetting = context.getProperty(JSON_TYPE).getValue();
        this.configureMapper(jsonTypeSetting);
        MongoCollection collection = this.getCollection(context);


        //TruongLX
        if (context.getProperty(QUERY_INCREMENTAL).isSet()) {
            try {
                int range = context.getProperty(RANGE_INCREMENTAL).isSet() ? context.getProperty(RANGE_INCREMENTAL).asInteger() : 1;
                String rsType = context.getProperty(TYPE_RESULT_INCREMENTAL).getValue();
                Date fromDate, finishDate;
                Calendar calendarTmp = Calendar.getInstance();
                try {
                    finishDate = FORMAT_DATE.parse(context.getProperty(TO_DATE).evaluateAttributeExpressions().getValue());
                } catch (Exception e) {
                    finishDate = calendarTmp.getTime();
                }
                try {
                    fromDate = FORMAT_DATE.parse(context.getProperty(FROM_DATE).evaluateAttributeExpressions().getValue());
                } catch (Exception e) {
                    calendarTmp.add(Calendar.DATE, -1);
                    fromDate = calendarTmp.getTime();
                }
                String query_incremental = context.getProperty(QUERY_INCREMENTAL).evaluateAttributeExpressions().getValue();
                Calendar calTo = Calendar.getInstance();
                Date toDate = calTo.getTime();
                while (fromDate.compareTo(finishDate) <= 0) {
                    try {
                        calTo.clear();
                        calTo.setTime(fromDate);
                        calTo.add(Calendar.DATE, range);
                        toDate = calTo.getTime();

                        if (query != null) {
                            query.append(query_incremental, new Document("$gte", fromDate).append("$lt", toDate));
                        } else {
                            query = new Document(query_incremental, new Document("$gte", fromDate).append("$lt", toDate));
                        }
                        FindIterable<Document> it = collection.find(query);
                        if (projection != null) {
                            it.projection(projection);
                        }

                        if (sort != null) {
                            it.sort(sort);
                        }

                        if (context.getProperty(LIMIT).isSet()) {
                            it.limit(context.getProperty(LIMIT).evaluateAttributeExpressions().asInteger());
                        }

                        if (context.getProperty(BATCH_SIZE).isSet()) {
                            it.batchSize(context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger());
                        }

                        final MongoCursor<Document> cursor = it.iterator();
                        ComponentLog log = this.getLogger();

                        try{
                            FlowFile flowFile = null;
                            if (!context.getProperty(RESULTS_PER_FLOWFILE).isSet()) {
                                while (cursor.hasNext()) {
                                    Document document = cursor.next();
                                    flowFile = session.create();
                                    flowFile = session.write(flowFile, new OutputStreamCallback() {
                                        public void process(OutputStream out) throws IOException {
                                            String json;
                                            if (jsonTypeSetting.equals("Standard")) {
                                                json = JsonProcessor.this.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(document);
                                            } else {
                                                json = document.toJson();
                                            }
                                            IOUtils.write(json, out);
                                        }
                                    });
                                    Map<String, String> stringMap = new HashMap<>();
                                    stringMap.put(CoreAttributes.MIME_TYPE.key(), rsType);
//                                stringMap.put(CoreAttributes.FILENAME.key(), FORMAT_DATE.format(fromDate));
                                    stringMap.put("address", document.getString("address"));
                                    flowFile = session.putAllAttributes(flowFile, stringMap);

                                    session.getProvenanceReporter().receive(flowFile, this.getURI(context));
                                    session.transfer(flowFile, REL_SUCCESS);
                                }
                            } else {
                                int ceiling = context.getProperty(RESULTS_PER_FLOWFILE).evaluateAttributeExpressions().asInteger();
                                if (ceiling >= 99999) {
                                    List<Document> documents = new ArrayList<>();
                                    try {
                                        while (cursor.hasNext()) {
                                            documents.add(cursor.next());
                                        }
                                        String payload;
                                        if (rsType.equals(JSON_TYPE_RESULT)) {
                                            payload = this.buildBatch(documents, "Standard");
                                        } else {
                                            payload = this.buildBatchCsvIncremental(documents);
                                        }
                                        if (payload != null && !payload.equals("") && !payload.equals("[]")) {
                                            flowFile = session.create();
                                            flowFile = session.write(flowFile, new OutputStreamCallback() {
                                                public void process(OutputStream out) throws IOException {
                                                    out.write(payload.getBytes("UTF-8"));
                                                }
                                            });
                                            Map<String, String> stringMap = new HashMap<>();
                                            stringMap.put(CoreAttributes.MIME_TYPE.key(), rsType);
                                            stringMap.put(CoreAttributes.FILENAME.key(), FORMAT_DATE.format(fromDate));
                                            flowFile = session.putAllAttributes(flowFile, stringMap);

                                            session.getProvenanceReporter().receive(flowFile, this.getURI(context));
                                            session.transfer(flowFile, REL_SUCCESS);
                                        }
                                    } catch (Exception e) {
                                        logger.error(fromDate.toString(), e);
                                    }
                                } else {
                                    ArrayList batch = new ArrayList();
                                    while (cursor.hasNext()) {
                                        batch.add(cursor.next());
                                        if (batch.size() == ceiling) {
                                            try {
                                                if (log.isDebugEnabled()) {
                                                    log.debug("Writing batch...");
                                                }

                                                String payload = this.buildBatch(batch, jsonTypeSetting);
                                                this.writeBatch(payload, context, session);
                                                batch = new ArrayList();
                                            } catch (IOException var22) {
                                                this.getLogger().error("Error building batch", var22);
                                            }
                                        }
                                    }

                                    if (batch.size() > 0) {
                                        try {
                                            this.writeBatch(this.buildBatch(batch, jsonTypeSetting), context, session);
                                        } catch (IOException var21) {
                                            this.getLogger().error("Error sending remainder of batch", var21);
                                        }
                                    }
                                }
                            }
                        } catch(Exception ex){
                            logger.error("Errorrrrrrrrrrrrrr.......",ex);
                        } finally {
                            cursor.close();
                        }
                    } catch(Exception ex){
                        logger.error("",ex);
                    } finally {
                        fromDate = toDate;
                    }
                }
                session.commit();
            } catch (
                    RuntimeException var24)

            {
                context.yield();
                session.rollback();
                logger.error("Failed to execute query {} due to {}", new Object[]{query, var24}, var24);
            }
//            try {
//                int range = context.getProperty(RANGE_INCREMENTAL).isSet() ? context.getProperty(RANGE_INCREMENTAL).asInteger() : 1;
//                String rsType = context.getProperty(TYPE_RESULT_INCREMENTAL).getValue();
//                Date fromDate, finishDate;
//                Calendar calendarTmp = Calendar.getInstance();
//                try {
//                    finishDate = FORMAT_DATE.parse(context.getProperty(TO_DATE).evaluateAttributeExpressions().getValue());
//                } catch (Exception e) {
//                    finishDate = calendarTmp.getTime();
//                }
//                try {
//                    fromDate = FORMAT_DATE.parse(context.getProperty(FROM_DATE).evaluateAttributeExpressions().getValue());
//                } catch (Exception e) {
//                    calendarTmp.add(Calendar.DATE, -1);
//                    fromDate = calendarTmp.getTime();
//                }
//                String query_incremental = context.getProperty(QUERY_INCREMENTAL).evaluateAttributeExpressions().getValue();
//                Document query1 = context.getProperty(QUERY).isSet() ? Document.parse(context.getProperty(QUERY).evaluateAttributeExpressions().getValue()) : null;
//                int limit = context.getProperty(LIMIT).isSet() ? context.getProperty(LIMIT).evaluateAttributeExpressions().asInteger() : -1;
//                Calendar calTo = Calendar.getInstance();
//                Date toDate = calTo.getTime();
//                while (fromDate.compareTo(finishDate) <= 0) {
//                    try {
//                        calTo.clear();
//                        calTo.setTime(fromDate);
//                        calTo.add(Calendar.DATE, range);
//                        toDate = calTo.getTime();
//
//                        if (query1 != null) {
//                            query1.append(query_incremental, new Document("$gte", fromDate).append("$lt", toDate));
//                        } else {
//                            query1 = new Document(query_incremental, new Document("$gte", fromDate).append("$lt", toDate));
//                        }
//
//                        FindIterable<Document> it = collection.find(query1);
//                        MongoCursor<Document> cursor = it.iterator();
//                        if (projection != null) {
//                            it.projection(projection);
//                        }
//
//                        if (sort != null) {
//                            it.sort(sort);
//                        }
//
//                        if (limit > 0) {
//                            it.limit(limit);
//                        }
//
//                        if (context.getProperty(BATCH_SIZE).isSet()) {
//                            it.batchSize(context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger());
//                        }
//                        FlowFile flowFile = null;
//                        if (!context.getProperty(RESULTS_PER_FLOWFILE).isSet()) {
//                            while (cursor.hasNext()) {
//                                flowFile = session.create();
//                                flowFile = session.write(flowFile, new OutputStreamCallback() {
//                                    public void process(OutputStream out) throws IOException {
//                                        String json;
//                                        if (jsonTypeSetting.equals("Standard")) {
//                                            json = JsonProcessor.this.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(cursor.next());
//                                        } else {
//                                            json = ((Document) cursor.next()).toJson();
//                                        }
//
//                                        IOUtils.write(json, out);
//                                    }
//                                });
//                                flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
//                                session.getProvenanceReporter().receive(flowFile, this.getURI(context));
//                                session.transfer(flowFile, REL_SUCCESS);
//                            }
//                        } else {
//                            int ceiling = context.getProperty(RESULTS_PER_FLOWFILE).evaluateAttributeExpressions().asInteger();
//                            if (ceiling >= 99999) {
//                                List<Document> documents = new ArrayList<>();
//                                try {
//                                    while (cursor.hasNext()) {
//                                        documents.add(cursor.next());
//                                    }
//                                    String payload;
//                                    if (rsType.equals(JSON_TYPE_RESULT)) {
//                                        payload = this.buildBatch(documents, "Standard");
//                                    } else {
//                                        payload = this.buildBatchCsvIncremental(documents);
//                                    }
//                                    if (payload != null && !payload.equals("")) {
//                                        flowFile = session.create();
//                                        flowFile = session.write(flowFile, new OutputStreamCallback() {
//                                            public void process(OutputStream out) throws IOException {
//                                                out.write(payload.getBytes("UTF-8"));
//                                            }
//                                        });
//                                        Map<String, String> stringMap = new HashMap<>();
//                                        stringMap.put(CoreAttributes.MIME_TYPE.key(), rsType);
//                                        stringMap.put(CoreAttributes.FILENAME.key(), FORMAT_DATE.format(fromDate));
//                                        flowFile = session.putAllAttributes(flowFile, stringMap);
//
//                                        session.getProvenanceReporter().receive(flowFile, this.getURI(context));
//                                        session.transfer(flowFile, REL_SUCCESS);
//                                    }
//                                } catch (Exception e) {
//                                    logger.error(fromDate.toString(), e);
//                                }
//                            }
//                        }
//                    } catch (Exception es) {
//                        logger.error("increment", es);
//                    } finally {
//                        fromDate = toDate;
//                        if (cursor != null) cursor.close();
//                    }
//                }
//                session.commit();
//            } catch (Exception ex) {
//                logger.error("QUERY INCREMENTAL", ex);
//                context.yield();
//                session.rollback();
//
//            }
//            return;
        } else {
            try

            {
                FindIterable<Document> it = query != null ? collection.find(query) : collection.find();
                if (projection != null) {
                    it.projection(projection);
                }

                if (sort != null) {
                    it.sort(sort);
                }

                if (context.getProperty(LIMIT).isSet()) {
                    it.limit(context.getProperty(LIMIT).evaluateAttributeExpressions().asInteger());
                }

                if (context.getProperty(BATCH_SIZE).isSet()) {
                    it.batchSize(context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger());
                }

                final MongoCursor<Document> cursor = it.iterator();
                ComponentLog log = this.getLogger();

                try {
                    FlowFile flowFile = null;
                    if (!context.getProperty(RESULTS_PER_FLOWFILE).isSet()) {
                        while (cursor.hasNext()) {
                            flowFile = session.create();
                            flowFile = session.write(flowFile, new OutputStreamCallback() {
                                public void process(OutputStream out) throws IOException {
                                    String json;
                                    if (jsonTypeSetting.equals("Standard")) {
                                        json = JsonProcessor.this.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(cursor.next());
                                    } else {
                                        json = ((Document) cursor.next()).toJson();
                                    }

                                    IOUtils.write(json, out);
                                }
                            });
                            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
                            session.getProvenanceReporter().receive(flowFile, this.getURI(context));
                            session.transfer(flowFile, REL_SUCCESS);
                        }
                    } else {
                        int ceiling = context.getProperty(RESULTS_PER_FLOWFILE).evaluateAttributeExpressions().asInteger();
                        ArrayList batch = new ArrayList();

                        while (cursor.hasNext()) {
                            batch.add(cursor.next());
                            if (batch.size() == ceiling) {
                                try {
                                    if (log.isDebugEnabled()) {
                                        log.debug("Writing batch...");
                                    }

                                    String payload = this.buildBatch(batch, jsonTypeSetting);
                                    this.writeBatch(payload, context, session);
                                    batch = new ArrayList();
                                } catch (IOException var22) {
                                    this.getLogger().error("Error building batch", var22);
                                }
                            }
                        }

                        if (batch.size() > 0) {
                            try {
                                this.writeBatch(this.buildBatch(batch, jsonTypeSetting), context, session);
                            } catch (IOException var21) {
                                this.getLogger().error("Error sending remainder of batch", var21);
                            }
                        }
                    }

                    session.commit();
                } finally {
                    cursor.close();
                }
            } catch (
                    RuntimeException var24)

            {
                context.yield();
                session.rollback();
                logger.error("Failed to execute query {} due to {}", new Object[]{query, var24}, var24);
            }

        }

    }

    static final String date = "2015-01-01T00:00:00.000Z";


    static {
        QUERY = (new PropertyDescriptor.Builder()).name("Query").description("The selection criteria; must be a valid MongoDB Extended JSON format; if omitted the entire collection will be queried").required(false).expressionLanguageSupported(true).addValidator(DOCUMENT_VALIDATOR).build();
        PROJECTION = (new PropertyDescriptor.Builder()).name("Projection").description("The fields to be returned from the documents in the result set; must be a valid BSON document").required(false).expressionLanguageSupported(true).addValidator(DOCUMENT_VALIDATOR).build();
        SORT = (new PropertyDescriptor.Builder()).name("Sort").description("The fields by which to sort; must be a valid BSON document").required(false).expressionLanguageSupported(true).addValidator(DOCUMENT_VALIDATOR).build();
        LIMIT = (new PropertyDescriptor.Builder()).name("Limit").description("The maximum number of elements to return").required(false).expressionLanguageSupported(true).addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).build();
        BATCH_SIZE = (new PropertyDescriptor.Builder()).name("Batch Size").description("The number of elements returned from the server in one batch").required(false).expressionLanguageSupported(true).addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).build();
        RESULTS_PER_FLOWFILE = (new PropertyDescriptor.Builder()).name("results-per-flowfile").displayName("Results Per FlowFile").description("How many results to put into a flowfile at once. The whole body will be treated as a JSON array of results.").required(false).expressionLanguageSupported(true).addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).build();
        JSON_EXTENDED = new AllowableValue("Extended", "Extended JSON", "Use MongoDB's \"extended JSON\". This is the JSON generated with toJson() on a MongoDB Document from the Java driver");
        JSON_STANDARD = new AllowableValue("Standard", "Standard JSON", "Generate a JSON document that conforms to typical JSON conventions instead of Mongo-specific conventions.");
        JSON_TYPE = (new PropertyDescriptor.Builder()).allowableValues(new AllowableValue[]{JSON_EXTENDED, JSON_STANDARD}).defaultValue("Extended").displayName("JSON Type").name("json-type").description("By default, MongoDB's Java driver returns \"extended JSON\". Some of the features of this variant of JSON may cause problems for other JSON parsers that expect only standard JSON types and conventions. This configuration setting  controls whether to use extended JSON or provide a clean view that conforms to standard JSON.").expressionLanguageSupported(false).required(true).build();

        //TruongLX
        QUERY_INCREMENTAL = (new PropertyDescriptor.Builder()).name("Field query incremental").description("File query incremental(datetime)").required(false).expressionLanguageSupported(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
        RANGE_INCREMENTAL = (new PropertyDescriptor.Builder()).name("Range").description("Number of date").required(false).expressionLanguageSupported(true).addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).defaultValue("1").build();
        FROM_DATE = (new PropertyDescriptor.Builder()).name("From Date").description("Value start(datetime:yyyy-MM-ddTHH:mm:ss.SSSZ").required(false).expressionLanguageSupported(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).defaultValue(date).build();
        TO_DATE = (new PropertyDescriptor.Builder()).name("To Date").description("Value start(datetime:yyyy-MM-ddTHH:mm:ss.SSSZ").required(false).expressionLanguageSupported(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).defaultValue(date).build();

        JSON_TYPE_RS = new AllowableValue(JSON_TYPE_RESULT, "JSON", "Flow file result json");
        CSV_TYPE_RS = new AllowableValue(CSV_TYPE_RESULT, "CSV", "Flow file result csv");

        TYPE_RESULT_INCREMENTAL = (new PropertyDescriptor.Builder()).allowableValues(new AllowableValue[]{JSON_TYPE_RS, CSV_TYPE_RS}).defaultValue(JSON_TYPE_RESULT).displayName("Result type").name("result-type").description("Result a flow file type: json or csv").expressionLanguageSupported(false).required(true).build();


        List<PropertyDescriptor> _propertyDescriptors = new ArrayList();
        _propertyDescriptors.add(URI);
        _propertyDescriptors.add(DATABASE_NAME);
        _propertyDescriptors.add(COLLECTION_NAME);
        _propertyDescriptors.add(SSL_CONTEXT_SERVICE);
        _propertyDescriptors.add(CLIENT_AUTH);
        _propertyDescriptors.add(JSON_TYPE);
        _propertyDescriptors.add(QUERY);
        _propertyDescriptors.add(PROJECTION);
        _propertyDescriptors.add(SORT);
        _propertyDescriptors.add(LIMIT);
        _propertyDescriptors.add(BATCH_SIZE);
        _propertyDescriptors.add(RESULTS_PER_FLOWFILE);
        _propertyDescriptors.add(SSL_CONTEXT_SERVICE);
        _propertyDescriptors.add(CLIENT_AUTH);

        //TruongLX:
        _propertyDescriptors.add(QUERY_INCREMENTAL);
        _propertyDescriptors.add(RANGE_INCREMENTAL);
        _propertyDescriptors.add(FROM_DATE);
        _propertyDescriptors.add(TO_DATE);
        _propertyDescriptors.add(TYPE_RESULT_INCREMENTAL);

        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);
        Set<Relationship> _relationships = new HashSet();
        _relationships.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(_relationships);

        FORMAT_DATE.setTimeZone(TimeZone.getTimeZone("UTC"));
    }
}

