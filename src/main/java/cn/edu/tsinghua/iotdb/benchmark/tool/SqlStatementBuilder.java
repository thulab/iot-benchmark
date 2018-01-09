package cn.edu.tsinghua.iotdb.benchmark.tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SqlStatementBuilder {
	private static final String STRING_DATA_TYPE = "TEXT";
	
	 /**
    *
    * @param line csv line data
    * @param timeseriesToType
    * @param deviceToColumn
    * @param colInfo
    * @param headInfo
    * @return
    * @throws IOException
    */
   public static List<String> createInsertSQL(String line,  Map<String, String> timeseriesToType,
   		Map<String, ArrayList<Integer>> deviceToColumn, List<String> colInfo, List<String> headInfo) throws IOException {
       String[] data = line.split(",", headInfo.size() + 1);
       List<String> sqls = new ArrayList<>();
       Iterator<Map.Entry<String, ArrayList<Integer>>> it = deviceToColumn.entrySet().iterator();
       while (it.hasNext()) {
           Map.Entry<String, ArrayList<Integer>> entry = it.next();
           StringBuilder sbd = new StringBuilder();
           ArrayList<Integer> colIndex = entry.getValue();
           sbd.append("insert into " + entry.getKey() + "(timestamp");
           int skipcount = 0;
           for (int j = 0; j < colIndex.size(); ++j) {
               if (data[entry.getValue().get(j) + 1].equals("")) {
                   skipcount++;
                   continue;
               }
               sbd.append(", " + colInfo.get(colIndex.get(j)));
           }
           // define every device null value' number, if the number equal the
           // sensor number, the insert operation stop
           if (skipcount == entry.getValue().size())
               continue;
           
           // TODO when timestampsStr is empty, 
           String timestampsStr = data[0];
           sbd.append(") values(").append(timestampsStr.trim().equals("") ? "NO TIMESTAMP" : timestampsStr);
//           if (timestampsStr.trim().equals("")) {
//               continue;
//           }
//           sbd.append(") values(").append(timestampsStr);

           for (int j = 0; j < colIndex.size(); ++j) {
               if (data[entry.getValue().get(j) + 1].equals(""))
                   continue;
               if (timeseriesToType.get(headInfo.get(colIndex.get(j))).equals(STRING_DATA_TYPE)) {
                   sbd.append(", \'" + data[colIndex.get(j) + 1] + "\'");
               } else {
                   sbd.append("," + data[colIndex.get(j) + 1]);
               }
           }
           sbd.append(")");
           sqls.add(sbd.toString());
       }
       return sqls;
   }

}
