package com.tl.test;

import com.tl.easb.utils.DateUtil;
import com.tl.demo.ColumnReadToList;
import org.springframework.core.io.ClassPathResource;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;

public class Te {

    private static String sql0 = "SELECT * FROM topic_copy where topicName= ?";
    private static String sql1 = "INSERT INTO topic (tableName, subId, sqlId,topicName)\n" +
            "VALUES\n" +
            "\t(?, ?, ?,?)";

    public static void main(String[] args) throws IOException {
        System.out.println( DateUtil.format(DateUtil.addDaysOfMonth(new Date(), 0), DateUtil.defaultDatePattern_YMD));
        new Te().readXml();
//        SimpleDateFormat sdf =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        System.out.println(sdf.format(new Date(1615997760000L)));
//        System.out.println(DateUtil.format(DateUtil.addDaysOfMonth(new Date(1615997760000L), 0), DateUtil.defaultDatePattern_YMD));
    }

    public static Document useDomReadXml(File file) {
//        File file = new File(soucePath);
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(file);
            return doc;
        } catch (Exception e) {
            System.err.println("读取该xml文件失败");
            e.printStackTrace();
        }
        return null;
    }

    private void readXml() throws IOException {
        try {
            ClassPathResource cl = new ClassPathResource("sql-mapping.xml");
//        ClassPathResource cl = new ClassPathResource("test.xml");
            String soucePath = "E:\\AllFilesISHere\\testfile\\books.xml";
            File file = cl.getFile();
            Document doc = Te.useDomReadXml(file);
            //读取xml内部节点集合
            org.w3c.dom.NodeList nlst = doc.getElementsByTagName("SqlMapping");
            Connection con = ColumnReadToList.getConnection();
            PreparedStatement ps = null;
            ResultSet rs = null;
            //遍历集合内容
            for (int i = 0; i < nlst.getLength(); i++) {

                String businessDataitemIds = doc.getElementsByTagName("businessDataitemIds").item(i).getFirstChild().getNodeValue().trim();
                String pnType = doc.getElementsByTagName("pnType").item(i).getFirstChild().getNodeValue().trim();
                String projectName = doc.getElementsByTagName("projectName").item(i).getFirstChild().getNodeValue().trim();
                String topicName = doc.getElementsByTagName("topicName").item(i).getFirstChild().getNodeValue().trim();
                String shardCount = doc.getElementsByTagName("shardCount").item(i).getFirstChild().getNodeValue().trim();
                String lifeCycle = doc.getElementsByTagName("lifeCycle").item(i).getFirstChild().getNodeValue().trim();
                String fields = doc.getElementsByTagName("fields").item(i).getFirstChild().getNodeValue().trim();

                StringBuffer sb = new StringBuffer();

                sb.append("ct bj_eas_cj ");
                sb.append(topicName);
                sb.append(" ");
                sb.append(2);
                sb.append(" ");
                sb.append(lifeCycle);
                sb.append(" (");
                String[] sbs = fields.split(",");
                for (String s : sbs
                ) {
                    String[] sA = s.split(":");
                    sb.append(sA[1] + " ");
                    sb.append(sA[0] + ",");
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.append(") ");
                System.out.println(sb.toString());

//                ps = con.prepareStatement(sql1);
//                insertInto(con,ps, topicName);
//                ps = con.prepareStatement(sql0);
//                rs = getBySql(con, ps, topicName);
//                String tableName = null;
//                String subId = null;
//                int type = 0;
//                while (rs.next()) {
//                    tableName = rs.getString("tableName");
//                    subId = rs.getString("subId");
//                    type = rs.getInt("sqlId");
//                }
//                if ("SD55646S".equals(subId))continue;
//                StringBuffer stringBuffer = new StringBuffer();
//                stringBuffer.append("<bean id=\"");
//                stringBuffer.append(topicName);
//                stringBuffer.append("\"\n" +
//                        "    class=\"com.tl.dataprocess.rdb.SingleSubscriptionAsyncExecutor\"\n" +
//                        "    init-method=\"init\">\n" +
//                        "        <property name=\"threadPoolSize\" value=\"0\"/>\n" +
//                        "        <property name=\"datahubClient\" ref=\"datahubClient\"/>\n" +
//                        "        <property name=\"jdbcTemplate\" ref=\"jdbcTemplate\"/>\n" +
//                        "        <property name=\"datahubPojectName\" value=\"hntl\"/>\n" +
//                        "        <property name=\"datahubTopicName\" value=\"");
//                stringBuffer.append(topicName);
//                stringBuffer.append("\"/>\n" +
//                        "        <property name=\"datahubSubId\" value=\"");
//                stringBuffer.append(subId);
//                stringBuffer.append("\"/>\n" +
//                        "        <!--\n" +
//                        "    三种选择：\n" +
//                        "            1、insert ignore into，插入不覆写原纪录\n" +
//                        "            2、replace into，插入并覆写原纪录\n" +
//                        "            3、insert into，正常插入，该方式要注意业务数据如果无法避免重复数据，则会抛出主键冲突异常，不建议使用\n" +
//                        "        -->\n" +
//                        "        <property name=\"sqlPrefix\" value=\"");
//                stringBuffer.append(getInserType(type));
//                stringBuffer.append("\"/>\n" +
//                        "        <property name=\"tableName\" value=\"");
//                stringBuffer.append(tableName);
//                stringBuffer.append("\"/>\n" +
//                        "        <property name=\"dbName\" value=\"easdb\"/>");
//                stringBuffer.append("\n</bean>");
//                System.out.println(stringBuffer.toString());
            }
//            JdbcUtils.close(con, ps, rs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void insertInto(Connection con, PreparedStatement ps, String topicName) throws Exception {
        String tp = topicName.toUpperCase();
        if (tp.contains("_SOURCE")) {
            tp = tp.substring(0, tp.length() - 7);
        }
        ps.setString(1, tp);
        ps.setString(2, "");
        ps.setInt(3, 3);
        ps.setString(4, topicName);
        ps.execute();
    }


    private ResultSet getBySql(Connection con, PreparedStatement ps, String topicName) throws Exception {
        ps.setString(1, topicName);
        ResultSet result = ps.executeQuery();
        return result;
    }

    private String getInserType(int type) {
        switch (type) {
            case 2:
                return "replace into";
            case 3:
                return "insert into";
            default:
                return "insert ignore into";
        }
    }

}
