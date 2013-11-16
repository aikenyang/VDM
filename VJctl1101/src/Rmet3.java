import java.io.*;
import java.text.*;
import java.util.*;
import java.util.concurrent.TimeUnit;


//import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import voldemort.client.*;
import voldemort.client.protocol.admin.*;
import voldemort.client.protocol.admin.filter.DefaultVoldemortFilter;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.utils.ByteArray;
//import voldemort.versioning.Versioned;

public class Rmet3 {
	public static void main(String[] args) {
		String host = "astest-listen1.dev.sea1.csh.tc";
		
		//RME Testing
        String bootStrapUrl = "tcp://"+host+":6666";
        String storeName = "Account_Store"; 
        int maxThreads = 300;
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setMaxThreads(maxThreads);
        clientConfig.setMaxConnectionsPerNode(maxThreads);
        clientConfig.setConnectionTimeout(5000, TimeUnit.MILLISECONDS);
        clientConfig.setBootstrapUrls(bootStrapUrl);
        StoreClientFactory factory = new SocketStoreClientFactory(clientConfig);
        StoreClient<String, String> client = factory.getStoreClient(storeName);
        //may change for different vdm node
        int nodeId = 2;
        List<Integer> partitionList = new ArrayList<Integer>();
        partitionList.add(2);
        partitionList.add(6);
        partitionList.add(10);
        partitionList.add(14);
        ///////////////
        AdminClient adminClient = new AdminClient(bootStrapUrl, new AdminClientConfig());
        VoldemortFilter vf =  new DefaultVoldemortFilter();
        Iterator<ByteArray> iterator = adminClient.bulkFetchOps.fetchKeys(nodeId, storeName, partitionList, vf, true);
        /////////////////above is vdm conneciton information
       
        String key = null;
        String value = null;
        //Versioned value = null;
        StringBuilder sb = new StringBuilder(); //for save a file
        
        String strBirthDateNo = null;
        Long LnEpoBDOld = null;
       // Long LnEpoBDNew = null;
		String strBirthYear = null;      
		String NewstrBirthDateNo = null;
		String strProperty = null;
		String NewFirstP = null;
		
        int count =0;
        
        while (iterator.hasNext()) {
            key = new String(iterator.next().get());
            value = client.getValue(key);
            strBirthDateNo = null; //default
            strProperty = null;	  //default
            JSONObject myjson = null;
            
            try {
            		myjson = new JSONObject(value);
                strBirthDateNo = myjson.getString("DateOfBirth");
                strProperty = myjson.getString("Property");
                System.out.println("Birthday String--> " + strBirthDateNo);
                System.out.println("strProperty String--> " + strProperty);
            } catch (JSONException e) {
            		//e.printStackTrace();
            		//System.out.println("Birthday String--> " + strBirthDateNo);
            		//System.out.println("strProperty String--> " + strProperty);
            }
   
			if (strBirthDateNo!=null && strBirthDateNo.contains("Date") ) {
				System.out.println("Key-Value-Pair::" + key + ":" + value);
				sb.append("Key-Value-Pair::" + key + ":" + value+"\r\n");
				
				strBirthDateNo = strBirthDateNo.substring(6, strBirthDateNo.length()-2); //get birthday string-epoch type
				System.out.println("strBirthDateNo--> " + strBirthDateNo);
				
				//native account & 1970/1/15 or 1970/1/16
				try{
					LnEpoBDOld = Long.valueOf(strBirthDateNo);
					//System.out.println("strEpoBDOld--> " + LnEpoBDOld);
				}
				catch (Exception e){					
				}
				//------
				int intPropertyV = 999; //default intPropertyV 
				
				if (strProperty != null){					
					try {
						intPropertyV = Integer.valueOf(strProperty);
						System.out.println("intPropertyV--> " + intPropertyV);
					}
					catch (Exception e){
						strProperty = null;
					}
				}
				
	            if ( 1209599000 < LnEpoBDOld &&  LnEpoBDOld < 138240000 && strProperty==null){
	            		NewstrBirthDateNo=null;
	            		System.out.println("Property null & birthday 1970 " );
	            }
	            else if (1209599000 < LnEpoBDOld &&  LnEpoBDOld < 1382400000 && intPropertyV==0){
	            		NewstrBirthDateNo=null;
	            		System.out.println("Property 0 & birthday 1970 " );
	            }	            
	            else if (-2208988800L <= LnEpoBDOld && LnEpoBDOld <=-2178144001L && (intPropertyV==1 | intPropertyV==2 | intPropertyV==3 | intPropertyV==4 )){
            			NewstrBirthDateNo=null;
            		System.out.println("Social Account " );
	            }                
	            else {
	            		strBirthYear = parseEpochTime(LnEpoBDOld);
			        System.out.println("BirthYEAR String--> " + strBirthYear);
		            //System.out.println("New BirthDate Number --> " + NewstrBirthDateNo);           
	            }
        			myjson.remove("DateOfBirth");
            		myjson.put("YearOfBirth", strBirthYear);
	            System.out.println("New Value String--> " + myjson);
	            sb.append("New Value String--> " + myjson+"\r\n");
	            
	            //write back to VDM
	            /*
	            try {
	            		client.put(key, myjson.toString());
	            }catch (Exception e){
	            		System.out.println("!!exception to put in VDM--> " + myjson);
	            		sb.append("!!exception to put in VDM--> " + myjson+"\r\n");
	            }
	            */
	            ///
	            count = count +1;
	            System.out.println(count);   
	            sb.append("count--> " + count+"\r\n");
	            sb.append("==================================" +"\r\n");
				}//birthday handle if
        }//key while loop
        
		try {
		    String Outputfile = "/tmp/"+host+".txt";
		    BufferedWriter fw = new BufferedWriter(new FileWriter(new File(Outputfile)));
		    fw.write(sb.toString());
		    fw.close();
		}
		
		catch (Exception e)
			{
			
		}
	}
	
	
	private static String parseEpochTime( Long LnEpoch ) {
		try {
        		Date date = null;     		
        		//if (intES==13 | intES==12){
        			date= new Date( LnEpoch);
	        //}
        		//else {
        		//	date = new Date( LnEpoch*1000);
        		//}
	        DateFormat aformat = new SimpleDateFormat("yyyy");
	        aformat.setTimeZone( TimeZone.getTimeZone("ETC/UTC") );
	        String strFormatted = aformat.format(date);
	        //System.out.println(strFormatted);
	        return strFormatted;	
        }      
        catch (Exception e) {
            String strFormatted = null;
            return strFormatted;   	
        }
	}

}
