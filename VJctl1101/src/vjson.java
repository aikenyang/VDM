import java.io.*;
import java.text.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
//import org.mortbay.log.Log;

import voldemort.client.*;
import voldemort.client.protocol.admin.*;
import voldemort.client.protocol.admin.filter.DefaultVoldemortFilter;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

public class vjson {

	public static void main(String[] args) {

		//L3, RCN stage
        String bootStrapUrl = "tcp://10.23.68.200:6666";
        
        //String storeName = "test";
        //String storeName = "DataCenter_Store";
        String storeName = "Account_Store"; //lot of time??
        //String storeName = "AuthApp_Store";
        //String storeName = "GlobalAccount_Store";
        
        int maxThreads = 300;
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setMaxThreads(maxThreads);
        clientConfig.setMaxConnectionsPerNode(maxThreads);
        clientConfig.setConnectionTimeout(5000, TimeUnit.MILLISECONDS);
        clientConfig.setBootstrapUrls(bootStrapUrl);

        StoreClientFactory factory = new SocketStoreClientFactory(clientConfig);
        StoreClient<String, String> client = factory.getStoreClient(storeName);

        int nodeId = 0;
        List<Integer> partitionList = new ArrayList<Integer>();
        partitionList.add(0);
        partitionList.add(4);
        partitionList.add(8);
        partitionList.add(12);
        
        AdminClient adminClient = new AdminClient(bootStrapUrl, new AdminClientConfig());
        VoldemortFilter vf =  new DefaultVoldemortFilter();
        //Iterator<ByteArray> iterator = adminClient.fetchKeys(nodeId, storeName, vf, partitionList, null);
        //BulkStreamingFetchOperations bsf = adm
        Iterator<ByteArray> iterator = adminClient.bulkFetchOps.fetchKeys(nodeId, storeName, partitionList, vf, true);
        
       
        String key = null;
        String value = null;
        //Versioned value = null;
        StringBuilder sb = new StringBuilder();
        
        String strBirthDateNo = null;
		String strBirthYear = null;      
		String NewstrBirthDateNo = null;
		String NewstrBirthDate = null;
		String NewFirstP = null;
		String PropertyVl = null;
        int count =0;
        
        while (iterator.hasNext()) {
            key = new String(iterator.next().get());
            value = client.getValue(key);
            System.out.println("Key, Value--> " + key +", "+ value.toString());
            //String subvalue = value.value("EmailAddress").toString();
            //String nValue = null;
            //nValue = value.toString().substring(1, value.toString().length()-1);
            //System.out.println("Value to String--> " + value.toString());
            //System.out.println("New Value--> " + nValue);
            //System.out.println("Value--> " + subvalue);
            String sbirth = null;
            
            try {
                JSONObject myjson = new JSONObject(value);

                JSONArray nameArray = myjson.names();
                sbirth = myjson.getString("DateOfBirth");
                JSONArray valArray = myjson.toJSONArray(nameArray);
                //sbirth = valArray.toString("Birthday");
                //for(int i=0;i<valArray.length();i++)
                //{
                		String nameList = nameArray.toString();
                    //String p = nameArray.getString(0) + "," + valArray.getString(0);
                    //Log.i("p",p);
                    //String p = nameArray.getString(0) + "," + valArray.getString(0);
                    System.out.println("NameList--> " + nameList);
                    System.out.println("Birthday String--> " + sbirth);
                    //}       

            } 
            catch (JSONException e) {
            		//e.printStackTrace();
            }
                 
        }
            
            
            
            
            

	}
	
   private static String parseEpochTime( String strEpochString ) {
        Date date = new Date( Double.valueOf( strEpochString+"000" ).longValue());
        //DateFormat aformat = new SimpleDateFormat("yyyy-MM-dd");
        DateFormat aformat = new SimpleDateFormat("yyyy");
        aformat.setTimeZone( TimeZone.getTimeZone("ETC/UTC") );
        String strFormatted = aformat.format(date);
        //System.out.println(strFormatted);
        return strFormatted;
	}
    

    
    private static String toEpochTime(String strDateTime)
    {
    		DateFormat df = new SimpleDateFormat("MM dd yyyy HH:mm:ss");
    		//df.setTimeZone( TimeZone.getTimeZone("ETC/UTC") );    		
        Date date = null;	
        try {
			date = df.parse("01 01 "+strDateTime+" 00:00:00");
		} 
		catch (ParseException e) {
			e.printStackTrace();
		}      
		long epoch = date.getTime();
        //System.out.println("epoch"+epoch/1000); // 1055545912454
        return Long.toString(epoch/1000);
    }

}
