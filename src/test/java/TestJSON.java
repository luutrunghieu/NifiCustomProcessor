import org.json.JSONArray;

public class TestJSON {
    public static void main(String[] args) {
        String json = "{name:\"a\"}";
        JSONArray jsonArray = new JSONArray(json);
        System.out.println(jsonArray);
    }
}
