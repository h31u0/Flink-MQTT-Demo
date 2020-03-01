package Sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

import com.google.gson.JsonObject;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class MysqlSinkAPP extends RichSinkFunction<JsonObject> {
    private PreparedStatement state ;
    private Connection conn ;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = getConnection();
        //System.out.println("conn=" + conn);
        String sql = "insert into `app_sensors`(device_id, type, start_time, end_time, avg, diff) values(?, ?, ?, ?, ?, ?);";
        state = this.conn.prepareStatement(sql);
        //System.out.println("state=" + state);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (conn != null) {
            conn.close();
        }
        if (state != null) {
            state.close();
        }
    }

    @Override
    public void invoke(JsonObject value, Context context) throws Exception {
        state.setString(1, value.get("device_id").getAsString());
        state.setString(2, value.get("type").getAsString());
        state.setTimestamp(3, new Timestamp(value.get("start_time").getAsLong()));
        state.setTimestamp(4, new Timestamp(value.get("end_time").getAsLong()));
        state.setString(5, value.get("avg").getAsString());
        state.setString(6, value.get("diff").getAsString());
        state.executeUpdate();
    }

    private static Connection getConnection() {
        Connection conn = null;
        try {
            String jdbc = "com.mysql.jdbc.Driver";
            Class.forName(jdbc);
            String url = "jdbc:mysql://192.168.8.109:3306/test";
            String user = "pi";
            String password = "raspberry";
            conn = DriverManager.getConnection(url +
                "?useUnicode=true" +
                "&useSSL=false" +
                "&characterEncoding=UTF-8" +
                "&serverTimezone=UTC",
                user,
                password);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }

}
