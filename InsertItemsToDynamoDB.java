import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class InsertItemsToDynamoDB {

    private static final String TABLE_NAME = "Customers";
    private static final int BATCH_SIZE = 25; // DynamoDB allows max 25 items per batch write
    private static final int TOTAL_ITEMS = 10000;

    public static void main(String[] args) {
        DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build();

        DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(dynamoDbClient)
                .build();

        insertItems(enhancedClient);

        dynamoDbClient.close();
    }

    private static void insertItems(DynamoDbEnhancedClient enhancedClient) {
        DynamoDbTable<Customer> customerTable = enhancedClient.table(TABLE_NAME, TableSchema.fromBean(Customer.class));

        List<Customer> customers = new ArrayList<>();
        for (int i = 0; i < TOTAL_ITEMS; i++) {
            Customer customer = new Customer();
            customer.setId(UUID.randomUUID().toString());
            customer.setName("Customer " + (i + 1));
            customer.setEmail("customer" + (i + 1) + "@example.com");
            customers.add(customer);

            // When we reach the batch size or the total items, write the batch
            if ((i + 1) % BATCH_SIZE == 0 || i == TOTAL_ITEMS - 1) {
                writeBatch(enhancedClient, customerTable, customers);
                customers.clear();
                System.out.println("Inserted " + (i + 1) + " items");
            }
        }
    }

    private static void writeBatch(DynamoDbEnhancedClient enhancedClient, DynamoDbTable<Customer> customerTable, List<Customer> customers) {
        WriteBatch.Builder<Customer> writeBuilder = WriteBatch.builder(Customer.class).mappedTableResource(customerTable);
        for (Customer customer : customers) {
            writeBuilder.addPutItem(customer);
        }

        BatchWriteItemEnhancedRequest batchWriteItemEnhancedRequest = BatchWriteItemEnhancedRequest.builder()
                .writeBatches(writeBuilder.build())
                .build();

        enhancedClient.batchWriteItem(batchWriteItemEnhancedRequest);
    }

    @DynamoDbBean
    public static class Customer {
        private String id;
        private String name;
        private String email;

        @DynamoDbPartitionKey
        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }
    }
}
