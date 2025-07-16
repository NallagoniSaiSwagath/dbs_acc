import org.apache.flink.table.api.*;

public class MidComplexityJob {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Source: sales
        tEnv.executeSql(
            "CREATE TABLE sales (" +
            "  customer_id STRING, " +
            "  product_id STRING, " +
            "  amount DOUBLE, " +
            "  txn_date DATE " +
            ") WITH (" +
            "  'connector'='filesystem'," +
            "  'path'='file:///path/to/sales.csv'," +
            "  'format'='csv')"
        );

        // Source: customers
        tEnv.executeSql(
            "CREATE TABLE customers (" +
            "  customer_id STRING, " +
            "  region STRING " +
            ") WITH (" +
            "  'connector'='filesystem'," +
            "  'path'='file:///path/to/customers.csv'," +
            "  'format'='csv')"
        );

        // Sink: daily_sales_summary
        tEnv.executeSql(
            "CREATE TABLE daily_sales_summary (" +
            "  region STRING, " +
            "  product_id STRING, " +
            "  total_sales DOUBLE " +
            ") WITH (" +
            "  'connector'='filesystem'," +
            "  'path'='file:///path/to/daily_sales_summary.csv'," +
            "  'format'='csv')"
        );

        // Transformation: join + filter + group by
        String sql = "INSERT OVERWRITE daily_sales_summary " +
                     "SELECT c.region, s.product_id, SUM(s.amount) AS total_sales " +
                     "FROM sales AS s " +
                     "JOIN customers AS c ON s.customer_id = c.customer_id " +
                     "WHERE s.txn_date = CURRENT_DATE " +
                     "GROUP BY c.region, s.product_id";

        tEnv.executeSql(sql).await();
    }
}
