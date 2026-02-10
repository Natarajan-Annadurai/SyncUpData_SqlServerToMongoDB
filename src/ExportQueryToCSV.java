import java.sql.*;
import java.io.FileWriter;
import java.io.IOException;

public class ExportQueryToCSV {
    public static void main(String[] args) {

        String url = "jdbc:sqlserver://localhost:1433;databaseName=MahilMart-Analytics;encrypt=false";
        String user = "mahilmartuser";        // change if using SQL auth
        String password = "Admin@123"; // change your password
        String outputFile = "D://MahilMartProductsData/Mahilmart_UpdatedData2.csv"; // where to save


        String query = """
                WITH LatestMRP AS (
                                                 SELECT\s
                                                     mrp.Item_No,
                                                     mrp.CloseQ,
                                                     ROW_NUMBER() OVER (
                                                         PARTITION BY mrp.Item_No\s
                                                         ORDER BY mrp.[Date] DESC
                                                     ) AS rn
                                                 FROM [MahilMart-Analytics].[dbo].[MRP_Table] mrp
                                                 WHERE mrp.CloseQ > 0
                                             ),
                                             LatestStk AS (
                                                 SELECT\s
                                                     stk.Item_No,
                                                     stk.CloseQ,
                                                     ROW_NUMBER() OVER (
                                                         PARTITION BY stk.Item_No\s
                                                         ORDER BY stk.curYear DESC
                                                     ) AS rn
                                                 FROM [MahilMart-Analytics].[dbo].[StkItem_Table] stk
                                                 WHERE stk.CloseQ > 0
                                             ),
                                             Combined AS (
                                                 SELECT\s
                                                     COALESCE(m.Item_No, s.Item_No) AS Item_No,
                                                     COALESCE(m.CloseQ, s.CloseQ) AS FinalCloseQ
                                                 FROM LatestMRP m
                                                 FULL OUTER JOIN LatestStk s\s
                                                     ON m.Item_No = s.Item_No\s
                                                    AND s.rn = 1
                                                 WHERE (m.rn = 1 OR m.rn IS NULL)
                                                   AND (s.rn = 1 OR s.rn IS NULL)
                                             )
                                             SELECT\s
                                                 itm.item_No,
                                                 itm.item_Name,
                                                 ig.itemGroup_Name,
                                                 b.brand_Name,
                                                 itm.Item_MrspRate,             \s
                                                 itm.Item_Code,
                                                 c.FinalCloseQ
                                             FROM Combined c
                                             JOIN [MahilMart-Analytics].[dbo].[Item_Table] itm\s
                                                 ON c.Item_No = itm.Item_No
                                             JOIN [MahilMart-Analytics].[dbo].[ItemGroup_Table] ig\s
                                                 ON itm.item_Group = ig.itemGroup_No
                                             JOIN [MahilMart-Analytics].[dbo].[Brand_Table] b\s
                                                 ON b.Brand_No = itm.Item_Brand;
               """
                ;

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query);
             FileWriter csvWriter = new FileWriter(outputFile)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();


            for (int i = 1; i <= columnCount; i++) {
                csvWriter.append(metaData.getColumnName(i));
                if (i < columnCount) csvWriter.append(",");
            }
            csvWriter.append("\n");


            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    csvWriter.append(rs.getString(i) == null ? "" : rs.getString(i));
                    if (i < columnCount) csvWriter.append(",");
                }
                csvWriter.append("\n");
            }

            System.out.println("Data exported successfully to " + outputFile);

        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }
}

