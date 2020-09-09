package com.adtiming.om.adc.util;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JdbcHelper {

    private JdbcHelper() {
    }

    public static long insertReturnId(JdbcTemplate jdbcTemplate, final String sql, final Object... params) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
                PreparedStatement ps = conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
                for (int i = 1; i <= params.length; i++)
                    ps.setObject(i, params[i - 1]);
                return ps;
            }
        }, keyHolder);
        return keyHolder.getKey().longValue();
    }

    public static void rollback(Connection con) {
        if (con == null)
            return;
        try {
            con.rollback();
        } catch (Exception e) {
        }
    }

}