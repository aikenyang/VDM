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

public class rcnstg {

	public static void main(String[] args) {

		//rcn  stage node2 (in L3) stage
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

        int nodeId = 1;
        List<Integer> partitionList = new ArrayList<Integer>();
        partitionList.add(1);
        partitionList.add(5);
        partitionList.add(9);
        partitionList.add(13);
        
        AdminClient adminClient = new AdminClient(bootStrapUrl, new AdminClientConfig());
        VoldemortFilter vf =  new DefaultVoldemortFilter();
        //Iterator<ByteArray> iterator = adminClient.fetchKeys(nodeId, storeName, vf, partitionList, null);
        //BulkStreamingFetchOperations bsf = adm
        Iterator<ByteArray> iterator = adminClient.bulkFetchOps.fetchKeys(nodeId, storeName, partitionList, vf, true);
        
       
        String key = null;
        //String value = null;
        Versioned value = null;
        StringBuilder sb = new StringBuilder();
        
        String strBirthDateNo = null;
		String strBirthDate = null;      
		String NewstrBirthDateNo = null;
		String NewstrBirthDate = null;
		String NewFirstP = null;
        int count =0;
        
        while (iterator.hasNext()) {
            key = new String(iterator.next().get());
            value = client.get(key);
            //System.out.println("Key-Value-Pair::" + key + ":" + value.toString());
            
			if (value.toString().contains( "DateOfBirth") ) {
				int intFirst = value.toString().indexOf("DateOfBirth") + 20;
				int intLast = value.toString().indexOf(")", intFirst);
				String strNewValue = null;
				
				strBirthDateNo = value.toString().substring(intFirst, intLast);
	            System.out.println("BirthDate Number --> " + strBirthDateNo);  
	            strBirthDate = parseEpochTime( strBirthDateNo );
	            System.out.println("BirthDate String--> " + strBirthDate); 
	            NewstrBirthDate = strBirthDate+"-01-01 00:00:00";
	            System.out.println("New BirthDate String--> " + NewstrBirthDate); 
	            NewstrBirthDateNo = toEpochTime( NewstrBirthDate);
	            System.out.println("New BirthDate Number --> " + NewstrBirthDateNo);
	            
	            System.out.println("Key-Value-Pair::" + key + ":" + value);
	            sb.append("Key-Value-Pair::" + key + ":" + value+"\r\n");
	            
	            //replace DateOfBirth to YearOfBirth
	            NewFirstP = value.toString().substring(0, intFirst).replace("DateOfBirth", "YearOfBirth");
	            strNewValue = NewFirstP + NewstrBirthDateNo + value.toString().substring(intLast) ;
	            System.out.println("New Value --> " + strNewValue);	
	            count = count +1;
	            System.out.println(count);
	            sb.append("New Value --> " + strNewValue+"\r\n");
	            sb.append("Count --> " + count+"\r\n");
				}
     
        }

	}
	
   private static String parseEpochTime( String strEpochString ) {
        Date date = new Date( Double.valueOf( strEpochString.replace(".", "") ).longValue() );
        //DateFormat aformat = new SimpleDateFormat("yyyy-MM-dd");
        DateFormat aformat = new SimpleDateFormat("yyyy");
        aformat.setTimeZone( TimeZone.getTimeZone("ETC/UTC") );
        String strFormatted = aformat.format(date);
        // System.out.println(formatted);
        return strFormatted;
	}
    

    
    private static String toEpochTime(String strDateTime)
    {
        // String str = "Jun 13 2003 23:11:52.454 UTC";
    		DateFormat df = new SimpleDateFormat(strDateTime);
    		df.setTimeZone( TimeZone.getTimeZone("ETC/UTC") );    		
    		
        Date date = null;
		try {
			date = df.parse(strDateTime);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        long epoch = date.getTime();
        //System.out.println(epoch/1000); // 1055545912454
        return Long.toString(epoch/1000);
    }

}
