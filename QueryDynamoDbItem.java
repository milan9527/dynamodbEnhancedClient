import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.Scanner;

public class QueryDynamoDbItem {

    private static final String TABLE_NAME = "Customers";

    public static void main(String[] args) {
        DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build();

        DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(dynamoDbClient)
                .build();

        DynamoDbTable<Customer> customerTable = enhancedClient.table(TABLE_NAME, TableSchema.fromBean(Customer.class));

        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter the customer ID to query: ");
        String customerId = scanner.nextLine();

        queryItem(customerTable, customerId);

        dynamoDbClient.close();
        scanner.close();
    }

    private static void queryItem(DynamoDbTable<Customer> customerTable, String customerId) {
        Key key = Key.builder().partitionValue(customerId).build();
        Customer customer = customerTable.getItem(key);

        if (customer != null) {
            System.out.println("Customer found:");
            System.out.println("ID: " + customer.getId());
            System.out.println("Name: " + customer.getName());
            System.out.println("Email: " + customer.getEmail());
        } else {
            System.out.println("No customer found with ID: " + customerId);
        }
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
