import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import java.sql.*;

public class SqlToMongoUpdate {
    public static void main(String[] args) {
        // --- SQL Server Connection ---
        String sqlUrl = "jdbc:sqlserver://localhost:1433;databaseName=MahilMart-Analytics;encrypt=false;";
        String sqlUser = "mahilmartuser";
        String sqlPass = "Admin@123";

        // --- MongoDB Connection ---
        String mongoUri = "mongodb+srv://mahiltechlabops_db_user:f2LGkyrgLO3klEMy@cluster0.qwlsmrq.mongodb.net/production?retryWrites=true&w=majority&appName=Cluster0";
        //String mongoUri = "mongodb+srv://manojkumarr_db_user:sU4uyZP5hd7EDTQ5@cluster0.7cxydxt.mongodb.net/mahilmartdb?retryWrites=true&w=majority&appName=Cluster0";
        String mongoDbName = "production";
        //String mongoDbName = "mahilmartdb";
        String mongoCollectionName = "products";

        try (
                Connection sqlConn = DriverManager.getConnection(sqlUrl, sqlUser, sqlPass);
                MongoClient mongoClient = MongoClients.create(mongoUri)
        ) {
            // SQL query to fetch data
            String fetchQuery =
                    "WITH LatestMRP AS (\n" +
                            "    SELECT mrp.Item_No, mrp.CloseQ,\n" +
                            "           ROW_NUMBER() OVER (PARTITION BY mrp.Item_No ORDER BY mrp.[Date] DESC) AS rn\n" +
                            "    FROM [MahilMart-Analytics].[dbo].[MRP_Table] mrp\n" +
                            "    WHERE mrp.CloseQ > 0\n" +
                            "),\n" +
                            "LatestStk AS (\n" +
                            "    SELECT stk.Item_No, stk.CloseQ,\n" +
                            "           ROW_NUMBER() OVER (PARTITION BY stk.Item_No ORDER BY stk.curYear DESC) AS rn\n" +
                            "    FROM [MahilMart-Analytics].[dbo].[StkItem_Table] stk\n" +
                            "    WHERE stk.CloseQ > 0\n" +
                            "),\n" +
                            "Combined AS (\n" +
                            "    SELECT COALESCE(m.Item_No, s.Item_No) AS Item_No,\n" +
                            "           COALESCE(m.CloseQ, s.CloseQ) AS FinalCloseQ\n" +
                            "    FROM LatestMRP m\n" +
                            "    FULL OUTER JOIN LatestStk s ON m.Item_No = s.Item_No AND s.rn = 1\n" +
                            "    WHERE (m.rn = 1 OR m.rn IS NULL) AND (s.rn = 1 OR s.rn IS NULL)\n" +
                            ")\n" +
                            "SELECT itm.Item_No,\n" +
                            "       itm.Item_MrspRate,\n" +
                            "       itm.Item_SalRate1,\n" +
                            "       c.FinalCloseQ\n" +
                            "FROM Combined c\n" +
                            "JOIN [MahilMart-Analytics].[dbo].[Item_Table] itm ON c.Item_No = itm.Item_No;";

            Statement stmt = sqlConn.createStatement();
            ResultSet rs = stmt.executeQuery(fetchQuery);

            // MongoDB collection
            MongoDatabase mongoDatabase = mongoClient.getDatabase(mongoDbName);
            MongoCollection<Document> collection = mongoDatabase.getCollection(mongoCollectionName);

            int count = 0;
            long totalMatched = 0;
            long totalModified = 0;
            // Iterate rows
            while (rs.next()) {
                int itemNo = rs.getInt("Item_No");
                double price = rs.getDouble("Item_MrspRate");
                double mahilmartPrice = rs.getDouble("Item_SalRate1");
                int stocks = rs.getInt("FinalCloseQ");

                // --- Update only if doc already exists ---
                UpdateResult result = collection.updateMany(
                        Filters.eq("item_No", itemNo),
                        Updates.combine(
                                Updates.set("price", price),
                                Updates.set("mahilmartPrice", mahilmartPrice),
                                Updates.set("stocks", stocks)
                        )
                );
                count++;
                totalMatched += result.getMatchedCount();
                totalModified += result.getModifiedCount();

                System.out.println("Num Of Items :" + count);
                System.out.println("ItemNo=" + itemNo +
                        " | Matched=" + result.getMatchedCount() +
                        " | Modified=" + result.getModifiedCount());
            }
            System.out.println("Total Matched: " + totalMatched);
            System.out.println("Total Modified: " + totalModified);
            rs.close();
            stmt.close();
            System.out.println("Sync completed successfully!");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
