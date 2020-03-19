// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Service
public class CountryService {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private JdbcTemplate jdbcTemplate;

    private Map<String, String> countryA3A2Map = Collections.emptyMap();

    @PostConstruct
    private void buildCountryMap() {
        try {
            Map<String, String> countryMap = new HashMap<>(300);
            jdbcTemplate.query("select a2,a3 from om_country", rs -> {
                countryMap.put(rs.getString("a3"), rs.getString("a2"));
            });
            countryA3A2Map = countryMap;
        } catch (Exception e) {
            LOG.error("buildCountryMap error", e);
        }
    }

    public String convertA3ToA2(String countryA3) {
        return countryA3A2Map.getOrDefault(countryA3, countryA3);
    }
}
