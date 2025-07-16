import org.apache.flink.table.api.*;

public class LowComplexityJob {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Source table
        tEnv.executeSql(
            "CREATE TABLE raw_customers (" +
            "  customer_id STRING, " +
            "  name STRING, " +
            "  age INT " +
            ") WITH (" +
            "  'connector'='filesystem'," +
            "  'path'='file:///path/to/raw_customers.csv'," +
            "  'format'='csv')"
        );

        // Sink table
        tEnv.executeSql(
            "CREATE TABLE cleaned_customers (" +
            "  customer_id STRING, " +
            "  name STRING, " +
            "  age INT " +
            ") WITH (" +
            "  'connector'='filesystem'," +
            "  'path'='file:///path/to/cleaned_customers.csv'," +
            "  'format'='csv')"
        );

        // Transformation: simple filter
        String sql = "INSERT OVERWRITE cleaned_customers " +
                     "SELECT customer_id, name, age " +
                     "FROM raw_customers " +
                     "WHERE age > 18";

        tEnv.executeSql(sql).await();
    }
}
