package features;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.mongodb.DBObject;

public class ServiceLayer {
//	reads from fileLocation txt file and outputs list of string containing each line's string as elements
	public static List<String> readFromTextFile(String fileLocation){
		List<String> responseList = new ArrayList<String>();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(fileLocation));
            String line;
            while ((line = br.readLine()) != null) {
                responseList.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return responseList;
	}
	
	//returns a string from a reader 
	  private static String readAll(Reader rd) throws IOException {
	    StringBuilder sb = new StringBuilder();
	    int cp;
	    while ((cp = rd.read()) != -1) {
	      sb.append((char) cp);
	    }
	    return sb.toString();
	  }
	
	  //reads a jsonArray from url and returns it
	  public static JsonArray readJsonFromUrl(String url) throws IOException {
	    InputStream is = new URL(url).openStream();
	    try {
	    	
		  BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));

	      String jsonText = readAll(rd);
	      //for reading a json from url uncomment the below two lines and comment out the next 3 lines
//	      JSONObject json = new JSONObject(jsonText);
//	      return json;
	      JsonParser  parser = new JsonParser();
	      JsonElement elem   = parser.parse(jsonText);
	      JsonArray elemArr = elem.getAsJsonArray();
	      
	      return elemArr;
	    } finally {
	      is.close();
	    }
	  }
}
