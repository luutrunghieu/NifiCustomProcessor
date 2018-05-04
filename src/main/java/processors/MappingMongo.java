package processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.mongodb.AbstractMongoProcessor;
import org.apache.nifi.processors.mongodb.ObjectIdSerializer;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"mongodb", "read", "get", "cdp", "mapping"})
public class MappingMongo extends AbstractMongoProcessor {
    public static final Validator DOCUMENT_VALIDATOR = (subject, value, context) -> {
        ValidationResult.Builder builder = new ValidationResult.Builder();
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

    private static List<PropertyDescriptor> properties;
    private static Set<Relationship> relationships;
    private ObjectMapper mapper;

    static final PropertyDescriptor QUERY;
    static final PropertyDescriptor PROJECTION;
    static final PropertyDescriptor SORT;

    static final String JSON_TYPE_EXTENDED = "Extended";
    static final String JSON_TYPE_STANDARD = "Standard";
    static final AllowableValue JSON_EXTENDED;
    static final AllowableValue JSON_STANDARD;
    static final PropertyDescriptor JSON_TYPE;

    // HieuLT4
    static final PropertyDescriptor FIELD_MAPPING_FROM;
    static final PropertyDescriptor FIELD_MAPPING_TO;
    static final PropertyDescriptor REPLACE_ID;
    static final AllowableValue REPLACE_ID_TRUE;
    static final AllowableValue REPLACE_ID_FALSE;

    //===============
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("success")
            .build();
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("fail")
            .description("fail")
            .build();

    public MappingMongo() {

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

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        ComponentLog logger = this.getLogger();
        Document query = context.getProperty(QUERY).isSet() ? Document.parse(context.getProperty(QUERY).evaluateAttributeExpressions().getValue()) : new Document();
        Document projection = context.getProperty(PROJECTION).isSet() ? Document.parse(context.getProperty(PROJECTION).evaluateAttributeExpressions().getValue()) : null;
        Document sort = context.getProperty(SORT).isSet() ? Document.parse(context.getProperty(SORT).evaluateAttributeExpressions().getValue()) : null;
        final String jsonTypeSetting = context.getProperty(JSON_TYPE).getValue();
        this.configureMapper(jsonTypeSetting);
        MongoCollection collection = this.getCollection(context);

        // HieuLT4
        String fieldMappingFrom = context.getProperty(FIELD_MAPPING_FROM).getValue();
        String fieldMappingTo = context.getProperty(FIELD_MAPPING_TO).getValue();
        Boolean replaceKey = context.getProperty(REPLACE_ID).asBoolean();

        final AtomicReference<String> value = new AtomicReference<>();

        FlowFile flowfile = session.get();
        try {
            session.read(flowfile, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {

                    String json = IOUtils.toString(in);
                    JSONArray sourceArray = new JSONArray(json);
                    for (int i = 0; i < sourceArray.length(); i++) {
                        JSONObject sourceObject = sourceArray.getJSONObject(i);
                        String valueMapping = sourceObject.getString(fieldMappingFrom);

                        try {
                            query.append(fieldMappingTo, valueMapping);
                            final FindIterable<Document> it = query != null ? collection.find(query) : collection.find();
                            if (projection != null) {
                                it.projection(projection);
                            }
                            if (sort != null) {
                                it.sort(sort);
                            }

                            final MongoCursor<Document> cursor = it.iterator();
                            ComponentLog log = getLogger();
                            try {
                                Document response = null;
                                while (cursor.hasNext()) {
                                    response = cursor.next();
                                    getLogger().info(response.toJson());
                                    for (String key : response.keySet()) {
                                        if (!(key.equalsIgnoreCase("_id") && replaceKey)) {
                                            if (!key.equalsIgnoreCase(fieldMappingTo)) {
                                                sourceObject.put(key, response.get(key));
                                            }
                                        }
                                    }
                                }
                            } finally {
                                cursor.close();
                            }
                        } catch (final RuntimeException e) {
                            context.yield();
                            session.rollback();
                            logger.error("Failed to execute query {} due to {}", new Object[]{query, e}, e);
                        }
                    }
                    value.set(sourceArray.toString());
                }
            });
        } catch (Exception ex) {
            ex.printStackTrace();
            getLogger().error("Failed to read json string.");
        }

        String results = value.get();
        if (results != null && !results.isEmpty()) {
            flowfile = session.putAttribute(flowfile, CoreAttributes.MIME_TYPE.key(), "application/json");
        }

        // To write the results back out ot flow file
        flowfile = session.write(flowfile, new OutputStreamCallback() {

            @Override
            public void process(OutputStream out) throws IOException {
                out.write(value.get().getBytes());
            }
        });

        session.transfer(flowfile, SUCCESS);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    static {
        QUERY = (new PropertyDescriptor.Builder()).name("Query").description("The selection criteria; must be a valid MongoDB Extended JSON format; if omitted the entire collection will be queried").required(false).expressionLanguageSupported(true).addValidator(DOCUMENT_VALIDATOR).build();
        PROJECTION = (new PropertyDescriptor.Builder()).name("Projection").description("The fields to be returned from the documents in the result set; must be a valid BSON document").required(false).expressionLanguageSupported(true).addValidator(DOCUMENT_VALIDATOR).build();
        SORT = (new PropertyDescriptor.Builder()).name("Sort").description("The fields by which to sort; must be a valid BSON document").required(false).expressionLanguageSupported(true).addValidator(DOCUMENT_VALIDATOR).build();
        JSON_EXTENDED = new AllowableValue("Extended", "Extended JSON", "Use MongoDB's \"extended JSON\". This is the JSON generated with toJson() on a MongoDB Document from the Java driver");
        JSON_STANDARD = new AllowableValue("Standard", "Standard JSON", "Generate a JSON document that conforms to typical JSON conventions instead of Mongo-specific conventions.");
        JSON_TYPE = (new PropertyDescriptor.Builder()).allowableValues(new AllowableValue[]{JSON_EXTENDED, JSON_STANDARD}).defaultValue("Extended").displayName("JSON Type").name("json-type").description("By default, MongoDB's Java driver returns \"extended JSON\". Some of the features of this variant of JSON may cause problems for other JSON parsers that expect only standard JSON types and conventions. This configuration setting  controls whether to use extended JSON or provide a clean view that conforms to standard JSON.").expressionLanguageSupported(false).required(true).build();

        //HieuLT4
        FIELD_MAPPING_FROM = new PropertyDescriptor.Builder().name("Field mapping from").description("Field mapping from").required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
        FIELD_MAPPING_TO = new PropertyDescriptor.Builder().name("Field mapping to").description("Field mapping to").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(false).build();

        REPLACE_ID_TRUE = new AllowableValue("true", "true", "replace id");
        REPLACE_ID_FALSE = new AllowableValue("false", "false", "not replace id");
        REPLACE_ID = new PropertyDescriptor.Builder().allowableValues(new AllowableValue[]{REPLACE_ID_FALSE,REPLACE_ID_TRUE}).name("Replace key").description("Replace id when mapping").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

        final List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(URI);
        _propertyDescriptors.add(DATABASE_NAME);
        _propertyDescriptors.add(COLLECTION_NAME);
        _propertyDescriptors.add(SSL_CONTEXT_SERVICE);
        _propertyDescriptors.add(CLIENT_AUTH);
        _propertyDescriptors.add(JSON_TYPE);
        _propertyDescriptors.add(QUERY);
        _propertyDescriptors.add(PROJECTION);
        _propertyDescriptors.add(SORT);
        _propertyDescriptors.add(SSL_CONTEXT_SERVICE);
        _propertyDescriptors.add(CLIENT_AUTH);
        _propertyDescriptors.add(FIELD_MAPPING_FROM);
        _propertyDescriptors.add(FIELD_MAPPING_TO);
        _propertyDescriptors.add(REPLACE_ID);
        properties = Collections.unmodifiableList(_propertyDescriptors);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(SUCCESS);
        _relationships.add(FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }
}
