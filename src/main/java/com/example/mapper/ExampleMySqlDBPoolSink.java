/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.example.mapper;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.example.domain.TextFileObj;
import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 *
 * @author chandanar
 */
public class ExampleMySqlDBPoolSink extends RichSinkFunction<TextFileObj> {

    //jdbc
    private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String CONNECTION_URL = "jdbc:mysql://localhost:3306/locations?useUnicode=true&characterEncoding=UTF-8";     

    //c3p0 datasource
    private ComboPooledDataSource dataSource;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = getDataSource();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (dataSource != null) {
            dataSource.close();
        }
    }

    @Override
    public void invoke(TextFileObj value, Context ctx) {
        PreparedStatement psUp = null;
        PreparedStatement psIn = null;
        Connection connection = null;

        String TBL_UP = "UPDATE locations.location_temp SET temperature = ?, recordDate = ? "
                + " WHERE location_id = ? ;";
        String TBL_IN = "INSERT INTO locations.location_temp (location_id, location_name, temperature, recordDate) "
                + "values (?, ?, ?, ?);";

        try {
            connection = dataSource.getConnection();
            psUp = connection.prepareStatement(TBL_UP);
            psIn = connection.prepareStatement(TBL_IN);

            //update promo tbl
            psUp.setDouble(1, value.getTemperature());
            psUp.setTimestamp(2, Timestamp.from(value.getRecordDate()));
            psUp.setLong(3, value.getLocationId());
            
            //insert tbl if update fails
            psIn.setLong(1, value.getLocationId());
            psIn.setString(2, value.getLocationName().toString());
            psIn.setDouble(3, value.getTemperature());
            psIn.setTimestamp(4, Timestamp.from(value.getRecordDate()));

            //upsert
            if (psUp.executeUpdate() == 0) {
                psIn.executeUpdate();
            }

        } catch (Exception e) {
            Logger.getLogger(ExampleMySqlDBPoolSink.class.getName()).log(Level.SEVERE, null, e);
        } finally {
            if (psUp != null) {
                try {
                    psUp.close();
                } catch (SQLException ex) {
                    Logger.getLogger(ExampleMySqlDBPoolSink.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (psIn != null) {
                try {
                    psIn.close();
                } catch (SQLException ex) {
                    Logger.getLogger(ExampleMySqlDBPoolSink.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    Logger.getLogger(ExampleMySqlDBPoolSink.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    public static ComboPooledDataSource getDataSource() throws PropertyVetoException {

        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass(JDBC_DRIVER);
        cpds.setJdbcUrl(CONNECTION_URL);
        cpds.setUser("username");
        cpds.setPassword("password");   

        // Optional Settings
        cpds.setInitialPoolSize(5);
        cpds.setMinPoolSize(5);
        cpds.setAcquireIncrement(2);
        cpds.setMaxPoolSize(50);
        cpds.setMaxStatements(100);

        //advanced settings
        cpds.setMaxConnectionAge(120);
        cpds.setMaxIdleTime(90);
        cpds.setIdleConnectionTestPeriod(30);
        cpds.setTestConnectionOnCheckin(true);
        cpds.setPreferredTestQuery("SELECT 1");

        return cpds;
    }

}
