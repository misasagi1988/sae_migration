package com.hansight.v5.migration;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

@Deprecated
//@Component
public class IntelligenceAdaptor implements Runnable {

    private final static Logger LOG = LoggerFactory.getLogger(Migration.class);

    @Autowired
    @Qualifier("mysqlJdbcTemplate")
    private JdbcTemplate mysqlTemplate;

    private final String hashString = "windows黑名单进程\n" +
            "内部邮箱域名\n" +
            "自定义垃圾邮件关键词\n" +
            "web服务动态网页地址";
    private final String emailString = "敏感数据普通人员权限\n" +
            "webshell backdoor detected\n" +
            "linux root用户组\n" +
            "网站robots_txt中地址\n" +
            "ek component\n" +
            "SQLNinja scanner activity detected\n" +
            "资产MAC列表\n" +
            "USB序列号列表\n" +
            "Executable download detected\n" +
            "常用管理员账号\n" +
            "Struts2漏洞利用\n" +
            "Microsoft Windows command line banner detected\n" +
            "重要服务器上监控的用户组\n" +
            "敏感数据管理员权限\n" +
            "Suspicious executable download detected\n" +
            "虚拟货币矿池域名\n" +
            "违规服务列表\n" +
            "恶意user-agent\n" +
            "色情网址列表\n" +
            "windows重要进程列表\n" +
            "已知服务列表\n" +
            "重要服务器常用进程\n" +
            "游戏列表\n" +
            "网站根目录\n" +
            "网站后台地址";

    @PostConstruct
    private void init() {
        new Thread(this).start();
    }

    @Override
    public void run() {
        List<String> hashList = Arrays.asList(StringUtils.split(hashString, "\n"));
        List<String> emailList = Arrays.asList(StringUtils.split(emailString, "\n"));
        LOG.info("hash group size: {}", hashList.size());
        LOG.info("email group size: {}", emailList.size());
        int hashErrCnt = 0;
        int emailErrCnt = 0;
        for(String hashName: hashList) {
            try {
                LOG.info("handle hash type: {}", hashName);
                List<IntelligenceGroup> igs = mysqlTemplate.query(String.format("select id, name, type, intelligence_type from security_intelligence_group where name like '%%%s%%'", hashName), new ListIGRowMapper());
                if (igs == null || igs.isEmpty()) {
                    LOG.error("{} not found", hashName);
                    continue;
                }
                IntelligenceGroup ig = igs.get(0);
                if (!ig.intelligenceType.equalsIgnoreCase("hash")) {
                    String sql = String.format("update security_intelligence_group set intelligence_type='hash' where id='%s'", ig.id);
                    mysqlTemplate.execute(sql);
                    hashErrCnt ++;
                }

                String igsql = String.format("select id, content, group_id from security_intelligence where group_id='%s'", ig.id);
                List<Intelligence> igList = mysqlTemplate.query(igsql, new ListIRowMapper());
                if (igList == null || igList.isEmpty()) {
                    LOG.info("{} has no context", hashName);
                    continue;
                }
                for(Intelligence ice: igList) {
                    if(StringUtils.containsIgnoreCase(ice.content, "请根据实际情况修改"))
                        continue;
                    Pattern ptn = Pattern.compile(ice.content);
                    if(!ptn.matcher(ice.content.replace("\\.",".")).find())
                        LOG.info("find invalid content {}, {}", ice.content, ice.id);
                }

            } catch (Exception e) {
                LOG.error("handle {} error", hashName);
                e.printStackTrace();
            }
        }

        for(String emailName: emailList) {
            try {
                LOG.info("handle email type: {}", emailName);
                List<IntelligenceGroup> igs = mysqlTemplate.query(String.format("select id, name, type, intelligence_type from security_intelligence_group where name='%s'", emailName), new ListIGRowMapper());
                if (igs == null || igs.isEmpty()) {
                    LOG.error("{} not found", emailName);
                    continue;
                }
                IntelligenceGroup ig = igs.get(0);
                if (!ig.intelligenceType.equalsIgnoreCase("email")) {
                    String sql = String.format("update security_intelligence_group set intelligence_type='email' where id='%s'", ig.id);
                    mysqlTemplate.execute(sql);
                    emailErrCnt ++;
                }
                String igsql = String.format("select id, content, group_id from security_intelligence where group_id='%s'", ig.id);
                List<Intelligence> igList = mysqlTemplate.query(igsql, new ListIRowMapper());
                if (igList == null || igList.isEmpty()) {
                    LOG.info("{} has no context", emailName);
                    continue;
                }
                for(Intelligence ice: igList) {
                    if(StringUtils.containsIgnoreCase(ice.content, "请根据实际情况修改"))
                        continue;
                    if(ice.content.contains("\\.")) {
                        ice.content = ice.content.replaceAll("\\\\\\.", ".");
                        if(!StringUtils.containsIgnoreCase(ice.content, ice.content))
                            LOG.error("find invalid content {}, {}", ice.content, ice.id);
                        String sql = String.format("update security_intelligence set content='%s' where id='%s'", ice.content, ice.id);
                        mysqlTemplate.execute(sql);
                    }
                }
            } catch (Exception e) {
                LOG.error("handle {} error", emailName);
                e.printStackTrace();
            }
        }

        LOG.info("end, old error cnt: hash: {}, email: {}", hashErrCnt, emailErrCnt);
        LOG.info("end, ");


    }

    class IntelligenceGroup {
        String id;
        String name;
        String type;
        String intelligenceType;
    }

    class Intelligence {
        String id;
        String content;
        String groupId;
    }

    class ListIGRowMapper implements RowMapper<IntelligenceGroup> {
        @Override
        public IntelligenceGroup mapRow(ResultSet rs, int rowNum) throws SQLException {
            IntelligenceGroup intelligenceGroup = new IntelligenceGroup();
            intelligenceGroup.id = rs.getString("id");
            intelligenceGroup.name = rs.getString("name");
            intelligenceGroup.type = rs.getString("type");
            intelligenceGroup.intelligenceType = rs.getString("intelligence_type");
            return intelligenceGroup;
        }
    }

    class ListIRowMapper implements RowMapper<Intelligence> {
        @Override
        public Intelligence mapRow(ResultSet rs, int rowNum) throws SQLException {
            Intelligence intelligence = new Intelligence();
            intelligence.id = rs.getString("id");
            intelligence.content = rs.getString("content");
            intelligence.groupId = rs.getString("group_id");
            return intelligence;
        }
    }
}
