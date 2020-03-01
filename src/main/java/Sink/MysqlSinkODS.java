package Sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

import com.google.gson.JsonObject;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class MysqlSinkODS extends RichSinkFunction<JsonObject> {
    private PreparedStatement state ;
    private Connection conn ;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = getConnection();
        //System.out.println("conn=" + conn);
        String sql = "insert into `ods_sensors`(device_id, humidity,pressure,temperature,acceleration,recorded_time) values(?, ?, ?, ?, ?, ?);";
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
        state.setString(1, value.get("Device_Name").getAsString());
        state.setString(2, value.get("Humidity").getAsString());
        state.setString(3, value.get("Pressure").getAsString());
        state.setString(4, value.get("Temperature").getAsString());
        state.setString(5, value.get("Acceleration").getAsString());
        state.setTimestamp(6, new Timestamp(value.get("Time").getAsLong()));
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
