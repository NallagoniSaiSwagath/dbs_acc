import org.apache.flink.table.api.*;

public class HighComplexityJob {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Source: user_profiles
        tEnv.executeSql(
            "CREATE TABLE user_profiles (" +
            "  region STRING, " +
            "  email STRING, " +
            "  last_login TIMESTAMP(3) " +
            ") WITH (" +
            "  'connector'='filesystem'," +
            "  'path'='file:///path/to/user_profiles.csv'," +
            "  'format'='csv')"
        );

        // Sink: latest_user_profiles
        tEnv.executeSql(
            "CREATE TABLE latest_user_profiles (" +
            "  region STRING, " +
            "  email_cleaned STRING, " +
            "  rank INT " +
            ") WITH (" +
            "  'connector'='filesystem'," +
            "  'path'='file:///path/to/latest_user_profiles.csv'," +
            "  'format'='csv')"
        );

        // Transformation: clean email + window row_number
        String sql =
            "INSERT OVERWRITE latest_user_profiles " +
            "SELECT region, " +
            "       LOWER(TRIM(email)) AS email_cleaned, " +
            "       rank_num " +
            "FROM ( " +
            "   SELECT region, email, " +
            "          ROW_NUMBER() OVER (PARTITION BY region ORDER BY last_login DESC) AS rank_num " +
            "   FROM user_profiles " +
            ") " +
            "WHERE rank_num = 1";

        tEnv.executeSql(sql).await();
    }
}
