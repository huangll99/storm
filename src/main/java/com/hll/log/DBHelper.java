package com.hll.log;

/**
 * Created by hll on 2016/6/13.
 */

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by hll on 2016/4/11.
 */
public class DBHelper {

  private static Logger logger = LoggerFactory.getLogger(DBHelper.class.getName());

  private static final BasicDataSource DATA_SOURCE;

  static {
    DATA_SOURCE = new BasicDataSource();
    DATA_SOURCE.setDriverClassName("com.mysql.jdbc.Driver");
    DATA_SOURCE.setUrl("jdbc:mysql://localhost:3306/demo");
    DATA_SOURCE.setUsername("root");
    DATA_SOURCE.setPassword("root");
  }

  public static Connection getConnection() {
    try {
      return DATA_SOURCE.getConnection();
    } catch (SQLException e) {
      logger.error("get connection error", e);
      throw new RuntimeException(e);
    }

  }

  public static void persist(Tuple tuple) {
    Connection connection = getConnection();
    PreparedStatement preparedStatement=null;
    try {
      preparedStatement= connection.prepareStatement("insert into apachelog(id,ip,dateTime,request,response,bytesSent,referrer,useragent,country,browser,os) values(null, ?, ?, ?,?, ?, ?, ?, ? , ?, ?)");
      preparedStatement.setString(1, tuple.getStringByField("ip"));
      preparedStatement.setString(2, tuple.getStringByField("dateTime"));
      preparedStatement.setString(3, tuple.getStringByField("request"));
      preparedStatement.setString(4, tuple.getStringByField("response"));
      preparedStatement.setString(5, tuple.getStringByField("bytesSent"));
      preparedStatement.setString(6, tuple.getStringByField("referrer"));
      preparedStatement.setString(7, tuple.getStringByField("useragent"));
      preparedStatement.setString(8, tuple.getStringByField("country"));
      preparedStatement.setString(9, tuple.getStringByField("browser"));
      preparedStatement.setString(10, tuple.getStringByField("os"));
      preparedStatement.executeUpdate();
      connection.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }finally {
      if (preparedStatement!=null){
        try {
          preparedStatement.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }
}