package processors;

import com.jayway.jsonpath.JsonPath;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"cdp","http","enrichment"})
@SideEffectFree
public class EnrichmentAddress extends AbstractProcessor {

    private static List<PropertyDescriptor> properties;
    private static Set<Relationship> relationships;
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Succes relationship")
            .build();
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Fail relationship")
            .build();
    public void getHttp(JSONObject json) throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        String address = json.getString("address");
        try {
            URIBuilder uriBuilder = new URIBuilder("https://kalinka.edumall.io/location_detect");
            uriBuilder.addParameter("s",address);
            HttpGet httpget = new HttpGet(uriBuilder.build());

            // Create a custom response handler
            ResponseHandler<String> responseHandler = new ResponseHandler<String>() {
                @Override
                public String handleResponse(
                        final HttpResponse response) throws ClientProtocolException, IOException {
                    int status = response.getStatusLine().getStatusCode();
                    if (status >= 200 && status < 300) {
                        HttpEntity entity = response.getEntity();
                        return entity != null ? EntityUtils.toString(entity) : null;
                    } else {
                        throw new ClientProtocolException("Unexpected response status: " + status);
                    }
                }

            };
            String responseBody = httpclient.execute(httpget, responseHandler);
            JSONObject responseObject = new JSONObject(responseBody);
            JSONObject payload = responseObject.getJSONObject("payload");
            JSONObject province = informationRetrival(payload.getJSONObject("province_detected"));
            JSONObject district = informationRetrival(payload.getJSONObject("district_detected"));
            JSONObject ward = informationRetrival(payload.getJSONObject("ward_detected"));
            json.put("province",province);
            json.put("district",district);
            json.put("ward",ward);
        } catch(Exception ex){
            ex.printStackTrace();
        }finally {
            httpclient.close();
        }
    }
    private JSONObject informationRetrival(JSONObject object){
        if(object!=null){
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("name",object.getString("name"));
            jsonObject.put("code",object.getString("code"));
            return jsonObject;
        }
        return null;
    }
    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession session) throws ProcessException {
        final AtomicReference<String> value = new AtomicReference<>();

        FlowFile flowfile = session.get();

        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try{
                    String json = IOUtils.toString(in);
                    JSONArray jsonArray = new JSONArray(json);
                    for(int i = 0;i<jsonArray.length();i++){
                        JSONObject jsonObject = jsonArray.getJSONObject(i);
                        getHttp(jsonObject);
                    }
                    value.set(jsonArray.toString());
                }catch(Exception ex){
                    ex.printStackTrace();
                    getLogger().error("Failed to read json string.");
                }
            }
        });
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

        final List<PropertyDescriptor> _properties = new ArrayList<>();
        properties = Collections.unmodifiableList(_properties);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(SUCCESS);
        _relationships.add(FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }
}
