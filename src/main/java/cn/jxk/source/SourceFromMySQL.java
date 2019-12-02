package cn.jxk.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Desc:
 * weixi: zhisheng_tian
 * blog: http://www.54tianzhisheng.cn/
 */
public class SourceFromMySQL extends RichSourceFunction<Student> {
    PreparedStatement ps;
    private Connection connection;

    /**
     * open() ⽅法中建⽴连接，这样不⽤每次 invoke 的时候都要建⽴连接和释放连
     * 接。
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "select * from Student;";
        ps = this.connection.prepareStatement(sql);
    }

    /**
     * 程序执⾏完毕就可以进⾏，关闭连接和释放资源的动作了
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) { //关闭连接和释放资源
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * DataStream 调⽤⼀次 run() ⽅法⽤来获取数据
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            Student student = new Student(
                    resultSet.getInt("id"),
                    resultSet.getString("name").trim(),
                    resultSet.getString("password").trim(),
                    resultSet.getInt("age"));
            ctx.collect(student);
        }
    }

    @Override
    public void cancel() {
    }

    private static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test? useUnicode = true & characterEncoding = UTF - 8", " root", " root123456");
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception, msg = " + e.getMessage());
        }
        return con;
    }
}
