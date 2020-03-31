// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.admob;

import com.adtiming.om.adc.dto.ReportAccount;
import com.adtiming.om.adc.service.AppConfig;
import com.adtiming.om.adc.web.BaseController;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

@RestController
@RequestMapping("/admob")
public class AuthAdmobController extends BaseController {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private AppConfig cfg;

    @Resource
    private JdbcTemplate jdbcTemplate;

    @RequestMapping("auth/getUrl")
    public String getUrl(int adnAppId) throws Exception {
        if (adnAppId < 1) {
            return null;
        }

        String sql = "select b.* from om_adnetwork_app a " +
                " inner join report_adnetwork_account b on (a.report_account_id=b.id)" +
                " where a.adn_id=2" +
                " and a.adn_app_key is not null and a.adn_app_key != ''" +
                " and a.id=?";
        List<ReportAccount> list = jdbcTemplate.query(sql, ReportAccount.ROWMAPPER, adnAppId);
        if (list.isEmpty()) {
            LOG.warn("admob auth failed,adnAppId:{}, msg:report_adnetwork_account not find", adnAppId);
            return null;
        }

        ReportAccount account = list.get(0);
        AuthAdmob auth = new AuthAdmob(cfg.proxy, cfg.authDir, account.adnApiKey, account.adnAppToken, account.credentialPath, cfg.authDomain);
        return auth.getUrl(account.authKey);
    }

    @RequestMapping("auth/callback/{authKey}")
    public String callback(String userId, String code, String error, @PathVariable("authKey") String authKey) throws Exception {
        String msg = "";
        try {
            String sql = "select * from report_adnetwork_account where auth_key=?";
            List<ReportAccount> list = jdbcTemplate.query(sql, ReportAccount.ROWMAPPER, authKey);
            if (list.isEmpty()) {
                return "account is not exsit";
            }
            ReportAccount account = list.get(0);
            AuthAdmob auth = new AuthAdmob(cfg.proxy, cfg.authDir, account.adnApiKey, account.adnAppToken, account.credentialPath, cfg.authDomain);
            auth.callback(authKey, code, error);
        } catch (Exception e) {
            LOG.error("callback error, appKey:{}", authKey, e);
            msg = "callback error,msg:" + e.getMessage();
        }
        return msg;
    }
}
