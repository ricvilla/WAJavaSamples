import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import com.microsoft.windowsazure.services.core.storage.*;
import com.microsoft.windowsazure.services.table.client.*;
import com.microsoft.windowsazure.services.table.client.TableQuery.*;

public class TableStorageSample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		createTable();
		//insertEntity();
		//insertEntities();
		retrieveTableEntities();
	}
	
	// Define the connection-string with your values
	public static final String storageConnectionString = 
		    "DefaultEndpointsProtocol=http;" + 
		    "AccountName=ricardo;" + 
		    "AccountKey=IeQWwRdpb1/V0BEEfITn5VKp0zcFCl2TJRplmtKS6AQe3NgAUpnfpuTWOgxR+YNQuNS+kTSuEJ+9tVl+4vs2ZA==";
	
	private static void createTable(){
		String tableName = "people";
		CloudTable cloudTable;
		try {
			CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
			CloudTableClient tableClient = storageAccount.createCloudTableClient();
			cloudTable = tableClient.getTableReference(tableName);
			cloudTable.createIfNotExist();
			
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void insertEntity(){
		// Retrieve storage account from connection-string
		CloudStorageAccount storageAccount;
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);
			// Create the table client.
			CloudTableClient tableClient = storageAccount.createCloudTableClient();

			// Create a new customer entity.
			CustomerEntity customer1 = new CustomerEntity("Harp","Walter");
			customer1.setEmail("Walter@contoso.com");
			customer1.setPhoneNumber("425-555-0101");
			customer1.setBirthday(new Date());

			// Create an operation to add the new customer to the people table.
			TableOperation insertCustomer1 = TableOperation.insert(customer1);

			// Submit the operation to the table service.
			tableClient.execute("people", insertCustomer1);

		} catch (InvalidKeyException | URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void insertEntities(){
		// Retrieve storage account from connection-string
		CloudStorageAccount storageAccount;
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);
			// Create the table client.
			CloudTableClient tableClient = storageAccount.createCloudTableClient();

			// Define a batch operation.
			TableBatchOperation batchOperation = new TableBatchOperation();

			// Create a customer entity to add to the table.
			CustomerEntity customer = new CustomerEntity("Smith", "Jeff");
			customer.setEmail("Jeff@contoso.com");
			customer.setPhoneNumber("425-555-0104");
			customer.setBirthday(createDate(1970,5,15));
			batchOperation.insert(customer);

			// Create another customer entity to add to the table.
			CustomerEntity customer2 = new CustomerEntity("Smith", "Ben");
			customer2.setEmail("Ben@contoso.com");
			customer2.setPhoneNumber("425-555-0102");
			customer2.setBirthday(createDate(1954,10,2));
			batchOperation.insert(customer2);

			// Create a third customer entity to add to the table.
			CustomerEntity customer3 = new CustomerEntity("Smith", "Denise");
			customer3.setEmail("Denise@contoso.com");
			customer3.setPhoneNumber("425-555-0103");
			customer3.setBirthday(createDate(1968,2,5));
			batchOperation.insert(customer3);

			// Execute the batch of operations on the "people" table.
			tableClient.execute("people", batchOperation);
		} catch (InvalidKeyException | URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
	}
	
	private static void retrieveTableEntities(){
		// Retrieve storage account from connection-string
		CloudStorageAccount storageAccount;
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);
			// Create the table client.
			CloudTableClient tableClient = storageAccount.createCloudTableClient();

			// Create a filter condition where the partition key is "Smith".
			String partitionFilter = TableQuery.generateFilterCondition(
			    TableConstants.PARTITION_KEY, 
			    QueryComparisons.EQUAL,
			    "Smith");

			// Create a filter condition where the row key is less than the letter "E".
			String rowFilter = TableQuery.generateFilterCondition(
			    TableConstants.ROW_KEY, 
			    QueryComparisons.EQUAL,
			    "FirstName");

			// Combine the two conditions into a filter expression.
			String combinedFilter = TableQuery.combineFilters(partitionFilter, 
			        Operators.AND, rowFilter);

			// Specify a range query, using "Smith" as the partition key,
			// with the row key being up to the letter "E".
			//TableQuery<CustomerEntity> rangeQuery =
			//    TableQuery.from("people", CustomerEntity.class)
			//    .where(combinedFilter);
			
			TableQuery<CustomerEntity> rangeQuery =
			    TableQuery.from("people", CustomerEntity.class);
			
			ArrayList<CustomerEntity> customerEntities = new ArrayList<CustomerEntity>();

			// Loop through the results, displaying information about the entity
			for (CustomerEntity entity : tableClient.execute(rangeQuery)) {
				customerEntities.add(entity);
			    System.out.println(entity.getPartitionKey() + " " + entity.getRowKey() + 
			        "\t" + entity.getEmail() + "\t" + entity.getPhoneNumber() + "\t" + entity.getBirthday().toString());
			}
			
			//Sort returned collection
			Collections.sort(customerEntities);
			
			for (CustomerEntity entity : customerEntities) {
			    System.out.println(entity.getPartitionKey() + " " + entity.getRowKey() + 
			        "\t" + entity.getEmail() + "\t" + entity.getPhoneNumber() + "\t" + entity.getBirthday().toString());
			}
			
		} catch (InvalidKeyException | URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static class CustomerEntity extends TableServiceEntity implements Comparable<CustomerEntity> {
	    public CustomerEntity(String lastName, String firstName) {
	        this.partitionKey = lastName;
	        this.rowKey = firstName;
	    }

	    public CustomerEntity() { }

	    String email;
	    String phoneNumber;
	    Date birthday;

	    public String getEmail() {
	        return this.email;
	    }

	    public void setEmail(String email) {
	        this.email = email;
	    }

	    public String getPhoneNumber() {
	        return this.phoneNumber;
	    }

	    public void setPhoneNumber(String phoneNumber) {
	        this.phoneNumber = phoneNumber;
	    }
	    
	    public Date getBirthday(){
	    	return this.birthday;
	    }
	    
	    public void setBirthday(Date birthday){
	    	this.birthday = birthday;
	    }
	    
	    public int compareTo(CustomerEntity customerEntity) {
	    	return birthday.compareTo(customerEntity.birthday);
	    }
	    
	}
	
	private static Date createDate(int year, int month, int day){
		String date = year + "/" + month + "/" + day;
	    java.util.Date utilDate = null;

	    try {
	      SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");
	      utilDate = formatter.parse(date);
	    } catch (ParseException e) {
	      System.out.println(e.toString());
	      e.printStackTrace();
	    }
	    
	    return utilDate;

	}

}
