package ghsmith.export4dg;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Example Windows usage:
 * c:\> type dataSet.1.17.2023.PathReportPull.csv | ^
 * java -cp uber-Export4DG-1.0-SNAPSHOT.jar ghsmith.export4dg.Annotator ^
 *   "jdbc:sqlserver://xxx;database=xxx;user=xxx;password=xxx" ^
 * > dataSet.1.17.2023.PathReportPull.annotated.csv
 * 
 * @author ghsmith
 */
public class Annotator {
    
    public static void main(String[] args) throws Exception {

        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection conn = DriverManager.getConnection(args[0]);

        PreparedStatement pstmt = conn.prepareStatement(
"select\n" +
"  s.specnum_formatted as accession_no, \n" +
"  us.name service,\n" +
"  convert(varchar, p.datetime_taken, 101) datetime_taken,\n" +
"  datediff(day, ?, p.datetime_taken) date_delta,\n" +
"  (select text_data from c_spec_text where specimen_id = s.specimen_id and texttype_id = '$final') as final_text\n" +
"from \n" +
"  c_specimen s \n" +
"  join c_specimen_ovf1 so on(s.specimen_id = so.specimen_id)\n" +
"  join c_d_user_specimen us on(so.user_dict_id = us.id)\n" +
"  join p_part p on(s.specimen_id = p.specimen_id and p.part_designator = 'A')\n" +
"where \n" +
"  s.patdemog_id in\n" +
"  (\n" +
"    select\n" +
"      rpd.patdemog_id\n" +
"    from\n" +
"      r_pat_demograph rpd\n" +
"      join r_medrec rm on(rpd.patdemog_id = rm.patdemog_id)\n" +
"    where\n" +
"      rpd.lastname = ?\n" +
"      and rm.medrec_num_stripped = ?\n" +
"  )\n" +
"  and us.name like '%Derm%'\n" +
"order by\n" +
"  p.datetime_taken"                
        );
        
        BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
        Pattern lmdPattern = Pattern.compile("^([0-9]*),.*,([^,]*),([^,]*),([^,]*)$");
        String line = stdIn.readLine();
        System.out.println(line);
        while((line = stdIn.readLine()) != null) {
            System.out.println(line);
            Matcher lmdMatcher = lmdPattern.matcher(line);
            if(lmdMatcher.matches()) {
                String id = lmdMatcher.group(1);
                String lastName = lmdMatcher.group(2);
                String mrn = lmdMatcher.group(3);
                String date = lmdMatcher.group(4);
                System.err.println(id);
                pstmt.clearParameters();
                pstmt.setString(1, date);
                pstmt.setString(2, lastName);
                pstmt.setString(3, mrn);
                try {
                    ResultSet rs = pstmt.executeQuery();
                    while(rs.next()) {
                        System.out.println(String.format("%s,\"%+06d|%s|%s|%s|%s\"",
                            id,
                            rs.getInt("date_delta"),
                            rs.getString("accession_no"),
                            rs.getString("service"),
                            rs.getString("datetime_taken"),
                            rs.getString("final_text") != null
                            ? (rs.getString("final_text")
                                .replace("\u0008", "") // there are ASCII 08 (backspace?) characters in this column
                                .replace("\r", "")
                                .replaceAll("\\s+$",""))
                                    .replaceAll("\n", "<BR/>")
                                    .replaceAll("\"", "&QUOT;")
                            : null

                        ));
                    }
                }
                catch(SQLException e) {
                    System.out.println(String.format("%s,\"%s\"",
                        id,
                        e.getMessage()
                    ));
                }
            }
        }
        
    }
    
}
