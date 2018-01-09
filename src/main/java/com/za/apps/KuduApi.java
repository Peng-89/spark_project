package com.za.apps;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.ArrayList;
import java.util.List;

public class KuduApi {
    public static void main(String[] args) throws Exception {
        KuduClient kuduClient = new KuduClient.KuduClientBuilder("10.18.67.111").defaultAdminOperationTimeoutMs(2000).build();

        String tableName = "test_table";
        List<ColumnSchema> columns = new ArrayList(2);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING)
                .build());
        List<String> rangeKeys = new ArrayList<String>();
        rangeKeys.add("key");

        Schema schema = new Schema(columns);
//        kuduClient.createTable(tableName, schema,
//                new CreateTableOptions().setRangePartitionColumns(rangeKeys));
        KuduTable table = kuduClient.openTable(tableName);
//        KuduSession session = kuduClient.newSession();
//        for (int i = 0; i < 10000; i++) {
//            Insert insert = table.newInsert();
//            PartialRow row = insert.getRow();
//            row.addInt(0, i);
//            row.addString(1, "value " + i);
//            session.apply(insert);
//        }
        List<Integer> columnIndexes = new ArrayList<Integer>();
        columnIndexes.add(Integer.valueOf(100));
        KuduScanner scanner= kuduClient.newScannerBuilder(table).setProjectedColumnIndexes(columnIndexes).build();
        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                System.out.println(result.getInt(0)+"\t "+result.getString(1));
            }
        }
    }
}
