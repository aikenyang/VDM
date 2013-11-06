import java.io.*;
import java.text.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import voldemort.client.*;
import voldemort.client.protocol.admin.*;
import voldemort.client.protocol.admin.filter.DefaultVoldemortFilter;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

public class ProdRmeApp1 {

	public static void main(String[] args) {

		//Prod RME, vdm connection information
        String bootStrapUrl = "tcp://idapp1.usw2.csh.tc:6666";
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
        int nodeId = 0;
        List<Integer> partitionList = new ArrayList<Integer>();
        partitionList.add(0);
        partitionList.add(10);
        partitionList.add(20);
        partitionList.add(30);
        ///////////////
        AdminClient adminClient = new AdminClient(bootStrapUrl, new AdminClientConfig());
        VoldemortFilter vf =  new DefaultVoldemortFilter();
        Iterator<ByteArray> iterator = adminClient.bulkFetchOps.fetchKeys(nodeId, storeName, partitionList, vf, true);
        /////////////////above is vdm conneciton information
       
        String key = null;
        //String value = null;
        Versioned value = null;
        StringBuilder sb = new StringBuilder(); //for save a file
        
        String strBirthDateNo = null;
		String strBirthYear = null;      
		String NewstrBirthDateNo = null;
		//String NewstrBirthDate = null;
		String NewFirstP = null;
		
        int count =0;
        
        while (iterator.hasNext()) {
            key = new String(iterator.next().get());
            value = client.get(key);
      
			if (value.toString().contains( "DateOfBirth") ) {
				System.out.println("Key-Value-Pair::" + key + ":" + value.toString());
				
				//get property value
				String PropertyVl = null;
	            if (value.toString().contains( "Property") ) {
					int intFt = value.toString().indexOf("Property") + 10;
	       			PropertyVl = value.toString().substring(intFt, intFt+1);
		            System.out.println("Property  --> " + PropertyVl); 
		            
	            }
				
				int intFirst = value.toString().indexOf("DateOfBirth") + 20;
				int intLast = value.toString().indexOf(")", intFirst);
				String strNewValue = null; //declare New Value String
				strBirthDateNo = value.toString().substring(intFirst, intLast); //get birthday string-epoch type
				long intBirthDateNo = 0;   //if intBirthDateNo=0 then skip
				
				//native account & 1970/1/16 or 1970/1/16
				try{
					intBirthDateNo = Long.valueOf(strBirthDateNo);
				}
				catch (Exception e){					
				}
				//------
				int intPropertyV = 999; //default intPropertyV 
				
				if (PropertyVl != null){					
					try {
						intPropertyV = Integer.valueOf(PropertyVl);
					}
					catch (Exception e){
						PropertyVl = null;
					}
				}
	            if ( 1209599 < intBirthDateNo &&  intBirthDateNo < 1382400 && PropertyVl==null){
	            		NewstrBirthDateNo=null;
	            		System.out.println("Property null & birthday 1970 " );
	            }
	            else if (1209599 < intBirthDateNo &&  intBirthDateNo < 1382400 && intPropertyV==0){
	            		NewstrBirthDateNo=null;
	            		System.out.println("Property 0 & birthday 1970 " );
	            }	            
	            else {
		            strBirthYear = parseEpochTime( strBirthDateNo );
		            System.out.println("BirthYEAR String--> " + strBirthYear); 
		            NewstrBirthDateNo = toEpochTime( strBirthYear);
		            System.out.println("New BirthDate Number --> " + NewstrBirthDateNo);
		            
		            //System.out.println("Key-Value-Pair::" + key + ":" + value);
		            //sb.append("Key-Value-Pair::" + key + ":" + value+"\r\n");     
	            }
	            
	            //replace DateOfBirth to YearOfBirth
	            if (intBirthDateNo == 0){	//exception data data not handle
	            		strNewValue = value.toString();
	            }
	            else {
		            NewFirstP = value.toString().substring(0, intFirst).replace("DateOfBirth", "YearOfBirth");
		            strNewValue = NewFirstP + NewstrBirthDateNo + value.toString().substring(intLast) ;
		            System.out.println("New Value --> " + strNewValue);	
		            //System.out.println("BirthDate Number --> " + strBirthDateNo); 
		            count = count +1;
		            System.out.println(count);
		            //sb.append("New Value --> " + strNewValue+"\r\n");
		            //sb.append("Count --> " + count+"\r\n");
	            }

	            
	            
				}
        }
        
		try {
		    //String Outputfile = "/tmp/ProdRmeApp1.txt";
		    //BufferedWriter fw = new BufferedWriter(new FileWriter(new File(Outputfile)));
		    //fw.write(sb.toString());
		    //fw.close();
		}
		
		catch (Exception e)
			{
			
		}
	}
	
   private static String parseEpochTime( String strEpochString ) {
        try {
        		//Integer intES = strEpochString.length();
        		int intES = strEpochString.length();
        		//String stringValue = Integer.toString(intES);
        		System.out.println("strEpochString length--> "+ intES);
        		
        		Date date = null;
        		
        		if (intES==13 | intES==12){
        			date= new Date( Double.valueOf( strEpochString).longValue());
	        }
        		else {
        			date = new Date( Double.valueOf( strEpochString+"000" ).longValue());
        		}

	        
	        //DateFormat aformat = new SimpleDateFormat("yyyy-MM-dd");
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
    

    
    private static String toEpochTime(String strDateTime)
    {
    		DateFormat df = new SimpleDateFormat("MM dd yyyy HH:mm:ss");
    		//df.setTimeZone( TimeZone.getTimeZone("ETC/UTC") );    		
        Date date = null;	
        try {
			date = df.parse("01 01 "+strDateTime+" 00:00:00");
		} 
		catch (ParseException e) {
			//e.printStackTrace();
			String strYear = null;
			return strYear;
		}      
		long epoch = date.getTime();
        //System.out.println("epoch"+epoch/1000); // 1055545912454
        return Long.toString(epoch/1000);
    }

}
