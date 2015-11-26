import java.io.IOException;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import org.json.JSONException;  
import org.json.JSONObject;  

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;

public class VoltdbBolt implements IRichBolt {
	
	private OutputCollector collector;
	private Connection conn;
	private Statement query;
	private String driver = "org.voltdb.jdbc.Driver";
	private String url = "jdbc:voltdb://192.168.112.31:21212,192.168.112.32:21212,192.168.112.33:21212,192.168.112.34:21212";


	public void cleanup() {
		// TODO Auto-generated method stub
		try {
			this.query.close();
			this.conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}


	public void execute(Tuple arg0) {
		// TODO Auto-generated method stub
		String line = arg0.getString(0);  
		
		if (line.startsWith("{DS.Input.")){
			String json= line.substring(line.indexOf('}') + 1);
			StringBuilder l = new StringBuilder(1024000);
			l.append("insert into event (uuid, user_id, gid, cid, session_id," + 
			"event_name,price,ip_address,ref_page,creation_time," + 
			"session2year,session6moth,session2hour,page_style,user_agent,java_support," + 
			"query_str, empty" + ") values(");
			if (convertLine(json, l)){
				try {      					
					query.execute(l.toString());					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					System.out.println(l.toString());
					e.printStackTrace();
				} 
			}
		}
		
        //collector.emit(arg0, new Values(val*2, val*3));
        collector.ack(arg0);
        
	}


	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		this.collector = arg2;
		try{
			Class.forName("org.voltdb.jdbc.Driver"); 
			Properties props = new Properties(); 
			props.setProperty("user", "cloud");
			props.setProperty("password", "cloud"); 
			
			this.conn = DriverManager.getConnection(url, props);
		
			this.query = conn.createStatement();
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}


	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("voltdb_key", "voltdb_details"));
	}


	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public  boolean convertLine(String json, StringBuilder l){
    	try{
        	JSONObject inputJSONObject = new JSONObject(json);
			if (!inputJSONObject.has("timestamp")){       						
				return false;
			}
			if (!inputJSONObject.has("cid")){
				return false;
			}
			if (!inputJSONObject.has("uuid")){
				return false;
			}
			
			l.append("'");
			l.append(inputJSONObject.getString("uuid"));			
			l.append("',");

			if (inputJSONObject.has("uid")){
				l.append("'");
				l.append(inputJSONObject.getString("uid"));
				l.append("',");
			}
			else
				l.append("null,");
			
			if (inputJSONObject.has("gid")){
				l.append("'");
				l.append(inputJSONObject.getString("gid"));
				l.append("',");
			}
			else
				l.append("null,");
			
			l.append("'");
			l.append(inputJSONObject.getString("cid"));			
			l.append("',");
			
			if (inputJSONObject.has("sid")){
				l.append("'");
				l.append(inputJSONObject.getString("sid"));
				l.append("',");
			}
			else
				l.append("null,");
			
			if (inputJSONObject.has("method")){
				l.append("'");
				l.append(inputJSONObject.getString("method"));
				l.append("',");
			}
			else
				l.append("null,");
			
			if (inputJSONObject.has("price")){
				l.append("'");
				l.append(inputJSONObject.getString("price"));
				l.append("',");
			}
			else
				l.append("null,");
			
			if (inputJSONObject.has("ip")){
				l.append("'");
				l.append(inputJSONObject.getString("ip"));
				l.append("',");
			}
			else
				l.append("null,");
			
			if (inputJSONObject.has("ref_page")){
				l.append("'");
				l.append(inputJSONObject.getString("ref_page").replace("\\", "\\\\"));
				l.append("',");
			}
			else
				l.append("null,");
			
			l.append(Math.round(1000000*inputJSONObject.getDouble("timestamp")));			
			l.append(",");
			
			if (inputJSONObject.has("tma")){
				l.append("'");
				l.append(inputJSONObject.getString("tma"));
				l.append("',");
			}
			else
				l.append("null,");
			
			if (inputJSONObject.has("tmd")){
				l.append("'");
				l.append(inputJSONObject.getString("tmd"));
				l.append("',");
			}
			else
				l.append("null,");
			
			if (inputJSONObject.has("tmc")){
				l.append("'");
				l.append(inputJSONObject.getString("tmc"));
				l.append("',");
			}
			else
				l.append("null,");
			
			if (inputJSONObject.has("p_t")){
				l.append("'");
				l.append(inputJSONObject.getString("p_t"));
				l.append("',");
			}
			else
				l.append("null,");
			
			if (inputJSONObject.has("user_agent")){
				l.append("'");
				l.append(inputJSONObject.getString("user_agent"));
				l.append("',");
			}
			else
				l.append("null,");
			
			if (inputJSONObject.has("ja")){
				if (inputJSONObject.getString("ja").compareToIgnoreCase("true") ==0 )
					l.append("1,");
				else
					l.append("0,");
			}
			else
				l.append("null,");
			

			if (inputJSONObject.has("qstr")){
				l.append("'");
				l.append(inputJSONObject.getString("qstr"));
				l.append("',");
			}
			else
				l.append("null,");
			
			if (inputJSONObject.has("emp")){
				if (inputJSONObject.getString("emp").compareToIgnoreCase("true") ==0 )
					l.append("1)");
				else
					l.append("0)");
			}
			else
				l.append("null)");
					  
        } catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
    	
    	return true;
    }
}
