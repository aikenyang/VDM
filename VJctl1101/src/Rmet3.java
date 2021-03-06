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
import voldemort.versioning.Versioned;

public class Rmet3 {
	public static void main(String[] args) {
		String host = "astest-listen2.dev.sea1.csh.tc";
		String storeName = "Account_Store";
		
		//RME Testing
        String bootStrapUrl = "tcp://"+host+":6666";
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
        /////////////////above is vdm connection information
       
        String key = null;
        String sValue = null;
        Versioned<String> vValue = null;
        
        //StringBuilder sb = new StringBuilder(); 		//records processed
        StringBuilder sb_nh = new StringBuilder(); 	//records skiped
        
        String strBirthDateNo = null;//get DateOfBirth form VDM, string
        Long LnEpoBDOld = null;		//get DateOfBirth form VDM, long
		String strProperty = null;  //get Property form VDM
		String strBirthYear = null; //get YearOfBirth form VDM    

       
		String strNYear = null;  	//generate new YearOfBirth to VDM
		
        int count =0;
        
        while (iterator.hasNext()) {
            key = new String(iterator.next().get());
            vValue = client.get(key);
            sValue = client.getValue(key);
            
            strBirthDateNo = null; //default, reset in every loop
            strProperty = null;	  //default, reset in every loop
            strBirthYear = null;	  //default, reset in every loop
            JSONObject myjson = null;
            
            try {
            		myjson = new JSONObject(sValue);
                try{
            			strBirthDateNo = myjson.getString("DateOfBirth");
            			//System.out.println("Birthday String--> " + strBirthDateNo);
                }	catch(JSONException e){}
                	
                try{
                		strProperty = myjson.getString("Property"); 	
                		//System.out.println("strProperty String--> " + strProperty);
	            }	catch(JSONException e){} 
            
                try{
	                	strBirthYear = myjson.getString("YearOfBirth"); 	
	                	//System.out.println("strBirthYear String--> " + strBirthYear);
                }	catch(JSONException e){}               
            } catch (JSONException e) {}
   
			if (strBirthDateNo!=null && strBirthDateNo.contains("Date") ) {
				System.out.println("Key-Value-Pair::" + key + ":" + sValue);
				//sb.append("Key-Value-Pair::" + key + ":" + value+"\r\n");
				
				strBirthDateNo = strBirthDateNo.substring(6, strBirthDateNo.length()-2); //get birthday string-epoch type
				//System.out.println("strBirthDateNo--> " + strBirthDateNo);			
				try{
					LnEpoBDOld = Long.valueOf(strBirthDateNo);
					//System.out.println("strEpoBDOld--> " + LnEpoBDOld);
				}
				catch (Exception e){	}
				//------
				int intPropertyV = 999; //default intPropertyV 
				
				if (strProperty != null){					
					try {
						intPropertyV = Integer.valueOf(strProperty);
						//System.out.println("intPropertyV--> " + intPropertyV);
					}
					catch (Exception e){
						strProperty = null;
					}
				}
				
				//native account & 1970/1/15 or 1970/1/16-Rule3 remove
	            if ( 1209599000 < LnEpoBDOld &&  LnEpoBDOld < 138240000 && strProperty==null){
	            		strNYear=null;
	            		System.out.println("Rule3, Property null & birthday 1970 " );
	            }
	            else if (1209599000 < LnEpoBDOld &&  LnEpoBDOld < 1382400000 && intPropertyV==0){
	            		strNYear=null;
	            		System.out.println("Rule3, Property 0 & birthday 1970 " );
	            }	            
	            else if (-2208988800L <= LnEpoBDOld && LnEpoBDOld <=-2178144001L && (intPropertyV==1 | intPropertyV==2 | intPropertyV==3 | intPropertyV==4 )){
	            		strNYear=null;
	            		System.out.println("Rule3, Social Account " );
	            }                
	            else {
	            		strNYear = parseEpochTime(LnEpoBDOld);
			        //System.out.println("BirthYEAR String--> " + strNYear);
		            //System.out.println("New BirthDate Number --> " + NewstrBirthDateNo);           
	            }
        			myjson.remove("DateOfBirth");
        			if (strBirthYear!=null && strNYear!=strBirthYear){
        				System.out.println("Rule4, not match--> " + myjson);
        	            sb_nh.append("Rule4, birthday year not match \r\n");
        	            sb_nh.append("Key-Value-Pair::" + key + ":" + sValue+"\r\n");
        	            sb_nh.append("====================================================\r\n");
        			}
            		myjson.put("YearOfBirth", strNYear);
	            System.out.println("New Value String-----> " + myjson);
	            //sb.append("New Value String--> " + myjson+"\r\n");
	            
	            //write back to VDM   
	                
	            try {
	            		vValue.setObject(myjson.toString());
	            		client.put(key, vValue);
	            }catch (Exception e){
	            		System.out.println("!!exception to put in VDM--> " + myjson);
	            		//sb.append("!!exception to put in VDM--> " + myjson+"\r\n");
	            }
	            ///
	            count = count +1;
	            System.out.println("count: "+count);
	            System.out.println("<================================> ");
	            //sb.append("count--> " + count+"\r\n");    
				}//if birthday contains 'Date' handle
			
			else if (strBirthDateNo!=null){
				System.out.println("Rules2--birthday noise remove & log, Key-Value-Pair::" + key + ":" + sValue);
				sb_nh.append("Rule2--birthday noise remove & log\r\n");
				sb_nh.append("Key-Value-Pair::" + key + ":" + sValue+"\r\n");
				sb_nh.append("====================================================\r\n");
				myjson.remove("DateOfBirth");
				
				//write back to VDM
				
	            try {
	            		vValue.setObject(myjson.toString());
	            		client.put(key, vValue);
		        }catch (Exception e){
		        		System.out.println("!!exception to put in VDM--> " + myjson);
		        		//sb.append("!!exception to put in VDM--> " + myjson+"\r\n");
		        }
			}
			

        ///
        //count = count +1;
        //System.out.println(count);   
        //sb.append("count--> " + count+"\r\n");
        //sb.append("==================================" +"\r\n");
			
			
			
        }//key while loop
        
		try {
		    String Outputfile = "/tmp/"+host+".txt";
		    BufferedWriter fw = new BufferedWriter(new FileWriter(new File(Outputfile)));
		    fw.write(sb_nh.toString());
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
