import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

import java.util.Iterator;

public class BatchDeleteClassQuestions {

    public static void main(String[] args) {
        Region region = Region.US_EAST_1; // Replace with your region if different
        DynamoDbClient ddb = DynamoDbClient.builder()
                .credentialsProvider(ProfileCredentialsProvider.create())
                .region(region)
                .build();

        DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(ddb)
                .build();

        String questionIdToDelete = "8e555809-f0e0-42b9-80dc-634c81c39647";

        deleteItem(enhancedClient, questionIdToDelete);

        ddb.close();
    }

    private static void deleteItem(DynamoDbEnhancedClient enhancedClient, String questionId) {
        try {
            DynamoDbTable<ClassQuestion> table = enhancedClient.table("ClassQuestions", TableSchema.fromBean(ClassQuestion.class));

            // Query the table to get the full key (including the sort key)
            QueryConditional queryConditional = QueryConditional
                    .keyEqualTo(Key.builder().partitionValue(questionId).build());

            QueryEnhancedRequest queryEnhancedRequest = QueryEnhancedRequest.builder()
                    .queryConditional(queryConditional)
                    .limit(1) // We expect only one item with this questionId
                    .build();

            Iterator<Page<ClassQuestion>> results = table.query(queryEnhancedRequest).iterator();

            if (results.hasNext()) {
                Page<ClassQuestion> page = results.next();
                if (!page.items().isEmpty()) {
                    ClassQuestion itemToDelete = page.items().get(0);

                    // Delete the item
                    DeleteItemEnhancedRequest deleteRequest = DeleteItemEnhancedRequest.builder()
                            .key(Key.builder()
                                    .partitionValue(itemToDelete.getQuestionId())
                                    .sortValue(itemToDelete.getTimestamp())
                                    .build())
                            .build();

                    table.deleteItem(deleteRequest);

                    System.out.println("Item with questionId " + questionId + " was successfully deleted.");
                } else {
                    System.out.println("No item found with questionId " + questionId);
                }
            } else {
                System.out.println("No item found with questionId " + questionId);
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @DynamoDbBean
    public static class ClassQuestion {
        private String questionId;
        private Long timestamp;

        @DynamoDbPartitionKey
        public String getQuestionId() { return questionId; }
        public void setQuestionId(String questionId) { this.questionId = questionId; }

        @DynamoDbSortKey
        public Long getTimestamp() { return timestamp; }
        public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }

        // Add other fields, getters, and setters as needed
    }
}
