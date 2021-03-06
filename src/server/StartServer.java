package server;

import bean.CustBatchJob;
import bean.RiseCusTotalInfo;
import com.boc.tool.common.GzipUtil;
import com.boc.tool.common.MD5Encode;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.zip.GZIPOutputStream;

/**
 * Created by Jean on 2017/4/22.
 */
public class StartServer {
    private static final Logger logger = Logger.getLogger(StartServer.class);

    private static final String DRIVER_MYSQL = "com.mysql.jdbc.Driver";
    private static final String URL_MYSQL = "jdbc:mysql://localhost:3306/test";
    private static final String USER_MYSQL = "hdss";
    private static final String PASSWORD_MYSQL = "hdss123";
    private static final String SQL_CBJC = "select * from ";
    private static final String SQL_CBJ = "select * from CUST_BATCH_JOB";
    private static final String SQL_AREANO = "select areano from code_table_areano where pvbno = ";
    private static final String SQL_CBJC_update = "update CUST_BATCH_JOB_CTL set STATUS = 1 where BATCHNO = ?" ;
    private static final String SQL_CBJR = "insert into CUST_BATCH_JOB_RECOUNT(BATCHNO, RECOUNT ,OPTIME,STATUS) " +
            "values(?,?,?,?) ";
    private static final String STATUS= "STATUS";
    private static final String BATCHNO = "CUSTGROUPNO";
    private static final String QUERYSQL = "QUERYSQL";

    private static final String DRIVER_PRESTO = "com.facebook.presto.jdbc.PrestoDriver";
    private static final String URL_PRESTO = "jdbc:presto://localhost:7070/hive/default";
    private static final String USER_PRESTO = "hdss";
    private static final String PASSWORD_PRESTO = "hdss2222";
    private static final String SCHEMA = "default";
    private static final String CATALOG = "mysql";
    private static final String MOBILE_NO = "MOBILE_NO";

    private static final String VALIDATE_PATH = "/Users/Jean/Work/HDSS/JavaHotDeploy/YueSheng/src/com/boc/tool/common/file";
    private static final String DATE_PATH = "/Users/Jean/Work/HDSS/JavaHotDeploy/YueSheng/src/com/boc/tool/common/file";
    private static final String CTL_PATH = "/Users/Jean/Work/HDSS/JavaHotDeploy/YueSheng/src/com/boc/tool/common/file";

    public static void main(String[] agrs) {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        List<String> listBatchno = new ArrayList<>();
        List<CustBatchJob> listCBJ = new ArrayList<>();
        List<String> areano= new ArrayList<>();
        List<List<RiseCusTotalInfo>> listRcti= new ArrayList<>();

        try {
            //1、加载mysql的驱动
            Class.forName(DRIVER_MYSQL);
        } catch (ClassNotFoundException e) {
            logger.error(e.getMessage(), e);
        }

        try {
            //2. 建立连接
           connection = DriverManager.getConnection(URL_MYSQL, USER_MYSQL, PASSWORD_MYSQL);
            //3. 数据库操作
            // 得到statement
            statement = connection.createStatement();
            //查询CUSTGROUP_BATCH_JOB_CTL的结果放在resultset中
            resultSet = statement.executeQuery(SQL_CBJC);
            while(resultSet.next()) {
                if( 1 == Integer.parseInt(resultSet.getString(STATUS))) {
                    listBatchno.add(resultSet.getString("BATCHNO"));
                }
            }
            //查询CUSTGROUP_BATCH_JOB中的QUERYSQL字段的结果和BATCHNO字段一起放在resultset中
            resultSet = statement.executeQuery(SQL_CBJ);
            while(resultSet.next()) {
            if(listBatchno.contains(resultSet.getString(BATCHNO))) {
                CustBatchJob cbj = new CustBatchJob();
                cbj.setBatchno(resultSet.getString(BATCHNO));
                cbj.setQuerysql(resultSet.getString(QUERYSQL));
                listCBJ.add(cbj);
            }

            }
            resultSet.close();
            statement.close();
            connection.close();
             } catch (SQLException e) {
            logger.error(e.getMessage(), e);
             }



            //获取HDFS的行内数据
            try {
                //1、加载presto的驱动
                Class.forName(DRIVER_PRESTO);
            } catch (ClassNotFoundException e) {
                logger.error(e.getMessage(), e);
            }

            try{
                //2. 建立连接
                connection = DriverManager.getConnection(URL_PRESTO, USER_PRESTO, PASSWORD_PRESTO);

                connection.setSchema(SCHEMA);
                connection.setCatalog(CATALOG);
                //3. 数据库操作
                // 得到statement
                statement = connection.createStatement();

                int i = 0;
                //查询CUSTGROUP_BATCH_JOB_CTL的结果放在resultset中
                for(CustBatchJob cbj : listCBJ) {
                    resultSet = statement.executeQuery(cbj.getQuerysql());
                    while(resultSet.next()) {
                        String ump_mobile_no = MD5Encode.getMD5().encode(resultSet.getString(MOBILE_NO));
                        String batchno = cbj.getBatchno();
                        listRcti.get(i).add(setRiseCusTotalInfo(resultSet, ump_mobile_no, batchno));
                    }
                    resultSet.previous();
                    resultSet = statement.executeQuery(SQL_AREANO + resultSet.getString("PROV_BR_NO"));
                    areano.add(i,"4" + resultSet.getString(0));
                    i++;
                }

                resultSet.close();
                statement.close();
                connection.close();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }

        //把HDFS中取出来的数据，生成三个文件。一个用于传入mysql，两个用来上传FTP。
        for(List<RiseCusTotalInfo> list :listRcti) {
        BufferedWriter outSql = null;
        BufferedWriter outDate = null;
        BufferedWriter outCtl = null;
        try {
            outSql = new BufferedWriter(new FileWriter(VALIDATE_PATH + listRcti.indexOf(list)) );
            outDate = new BufferedWriter(new FileWriter( DATE_PATH +
                    "03603005_Gup10020_" + list.get(0).getBatchno() + "_" + areano.get(listRcti.indexOf(list))
                    + "_" + new SimpleDateFormat("yyyyMMdd").format(new Date()) + "_" +
                            new SimpleDateFormat("YYYYMMDDHHMMssSSS").format(new Date()) + "_DAT"
            ));
            outCtl =  new BufferedWriter(new FileWriter( CTL_PATH +
                    "03603005_Gup10020_" + list.get(0).getBatchno() + "_" + areano.get(listRcti.indexOf(list))
                            + "_" + new SimpleDateFormat("yyyyMMdd").format(new Date()) + "_" +
                            new SimpleDateFormat("YYYYMMDDHHMMssSSS").format(new Date()) + "_CTL"
            ));
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        for(RiseCusTotalInfo riseCusTotalInfo: list)
        {
            try {
                outSql.write(riseCusTotalInfo.getBatchno()
                        + "|" +
                        riseCusTotalInfo.getUmp_mobile_no()
                        + "|" +
                        riseCusTotalInfo.getCustomer_no()
                        + "|" +
                        riseCusTotalInfo.getMobile_no()
                        + "|" +
                        riseCusTotalInfo.getMobile_chnl()
                        + "|" +
                        riseCusTotalInfo.getMain_id_type()
                        + "|" +
                        riseCusTotalInfo.getMain_id_no()
                        + "|" +
                        riseCusTotalInfo.getCustomer_name()
                        + "|" +
                        riseCusTotalInfo.getCustomer_type()
                        + "|" +
                        riseCusTotalInfo.getProv_br_no()
                        + "|" +
                        riseCusTotalInfo.getCus_branch_no()
                        + "|" +
                        riseCusTotalInfo.getBr_name()
                        + "|" +
                        riseCusTotalInfo.getCreate_brno()
                        + "|" +
                        riseCusTotalInfo.getSecond_branch_no()
                        + "|" +
                        riseCusTotalInfo.getSecond_br_name()
                        + "|" +
                        riseCusTotalInfo.getAddress()
                        + "|" +
                        riseCusTotalInfo.getPhone_no_off()
                        + "|" +
                        riseCusTotalInfo.getPhone_no_house()
                        + "|" +
                        riseCusTotalInfo.getNationality_cd()
                        + "|" +
                        riseCusTotalInfo.getTitle_code()
                        + "|" +
                        riseCusTotalInfo.getMarital_status()
                        + "|" +
                        riseCusTotalInfo.getPosition()
                        + "|" +
                        riseCusTotalInfo.getOccupation()
                        + "|" +
                        riseCusTotalInfo.getBirthday()
                        + "|" +
                        riseCusTotalInfo.getAge()
                        + "|" +
                        riseCusTotalInfo.getEmployer()
                        + "|" +
                        riseCusTotalInfo.getEmpl_addr()
                        + "|" +
                        riseCusTotalInfo.getRisk_grade()
                        + "|" +
                        riseCusTotalInfo.getCus_level()
                        + "|" +
                        riseCusTotalInfo.getPt_card_flg()
                        + "|" +
                        riseCusTotalInfo.getPhone_bank_flg()
                        + "|" +
                        riseCusTotalInfo.getBusiness_flg()
                        + "|" +
                        riseCusTotalInfo.getMobile_bank_flg()
                        + "|" +
                        riseCusTotalInfo.getDeposit_flag()
                        + "|" +
                        riseCusTotalInfo.getNet_bank_flg()
                        + "|" +
                        riseCusTotalInfo.getCard_flg()
                        + "|" +
                        riseCusTotalInfo.getMobile_msg_flag()
                        + "|" +
                        riseCusTotalInfo.getCredit_card_flg()
                        + "|" +
                        riseCusTotalInfo.getXpad_acct_cny_flag()
                        + "|" +
                        riseCusTotalInfo.getXpad_acct_other_flag()
                        + "|" +
                        riseCusTotalInfo.getTpcc_flag()
                        + "|" +
                        riseCusTotalInfo.getFund_flag()
                        + "|" +
                        riseCusTotalInfo.getFnacno_flag()
                        + "|" +
                        riseCusTotalInfo.getMetal_flag()
                        + "|" +
                        riseCusTotalInfo.getBond_flag()
                        + "|" +
                        riseCusTotalInfo.getDeposit_amt()
                        + "|" +
                        riseCusTotalInfo.getLoan_amt()
                        + "|" +
                        riseCusTotalInfo.getFund_amt()
                        + "|" +
                        riseCusTotalInfo.getBond_amt()
                        + "|" +
                        riseCusTotalInfo.getIbas_amt()
                        + "|" +
                        riseCusTotalInfo.getGold()
                        + "|" +
                        riseCusTotalInfo.getTpcc_amt()
                        + "|" +
                        riseCusTotalInfo.getFinance_in()
                        + "|" +
                        riseCusTotalInfo.getFinance_out()
                        + "|" +
                        riseCusTotalInfo.getFinance_other()
                        + "|" +
                        riseCusTotalInfo.getMetal_amt()
                        + "|" +
                        riseCusTotalInfo.getCard_amt()
                        + "|" +
                        riseCusTotalInfo.getAum_amt()
                        + "|" +
                        riseCusTotalInfo.getCurrency_sum()
                        + "|" +
                        riseCusTotalInfo.getThre_mon_c_tran_amount()
                        + "|" +
                        riseCusTotalInfo.getSix_mon_c_tran_amount()
                        + "|" +
                        riseCusTotalInfo.getTwlv_mon_c_tran_amount()
                        + "|" +
                        riseCusTotalInfo.getThre_mon_d_tran_amount()
                        + "|" +
                        riseCusTotalInfo.getSix_mon_d_tran_amount()
                        + "|" +
                        riseCusTotalInfo.getTwlv_mon_d_tran_amount()
                        + "|" +
                        riseCusTotalInfo.getThre_mon_fina_peak()
                        + "|" +
                        riseCusTotalInfo.getSix_mon_fina_peak()
                        + "|" +
                        riseCusTotalInfo.getTwlv_mon_fina_peak()
                        + "|" +
                        riseCusTotalInfo.getYear_avg_fina_assets());
                outSql.newLine();

                outDate.write(riseCusTotalInfo.getUmp_mobile_no());
                outDate.newLine();
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
            GzipUtil.compressFile(DATE_PATH +
                    "03603005_Gup10020_" + list.get(0).getBatchno() + "_" + areano.get(listRcti.indexOf(list))
                    + "_" + new SimpleDateFormat("yyyyMMdd").format(new Date()) + "_" +
                    new SimpleDateFormat("YYYYMMDDHHMMssSSS").format(new Date()) + "_DAT");
            long fileLenth = new File(DATE_PATH +
                    "03603005_Gup10020_" + list.get(0).getBatchno() + "_" + areano.get(listRcti.indexOf(list))
                    + "_" + new SimpleDateFormat("yyyyMMdd").format(new Date()) + "_" +
                    new SimpleDateFormat("YYYYMMDDHHMMssSSS").format(new Date()) + "_DAT.gz").length();
            try {
                outCtl.write(list.size() + "|1|" +fileLenth);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }

        //导入数据

        //更新状态表
        try {
            //1、加载mysql的驱动
            Class.forName(DRIVER_MYSQL);
        } catch (ClassNotFoundException e) {
            logger.error(e.getMessage(), e);
        }


        PreparedStatement ps;

        try {
            //2. 建立连接
            connection = DriverManager.getConnection(URL_MYSQL, USER_MYSQL, PASSWORD_MYSQL);
            //3. 数据库操作
            // 得到statement
            ps = connection.prepareStatement(SQL_CBJC_update);
            //查询CUSTGROUP_BATCH_JOB_CTL的结果放在resultset中
            statement.executeQuery(SQL_CBJC_update);

            for(String batchno:listBatchno) {
                ps.setString(1, batchno);
                ps.executeUpdate();
            }

            resultSet.close();
            statement.close();
            connection.close();
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }

        //更新另一个表
        try{
        ps = connection.prepareStatement(SQL_CBJR);
        //查询CUSTGROUP_BATCH_JOB_CTL的结果放在resultset中
        statement.executeQuery(SQL_CBJC_update);

        for(List<RiseCusTotalInfo> list :listRcti) {

                ps.setString(1, list.get(0).getBatchno());
                ps.setString(2, String.valueOf(list.size()));
                ps.setString(3, new SimpleDateFormat("YYYYMMDDHHMMss").format(new Date()));
                ps.setString(4, String.valueOf(1));
                ps.executeQuery();
        }

        resultSet.close();
        statement.close();
        connection.close();
    } catch (SQLException e) {
        logger.error(e.getMessage(), e);
    }

    }

    private static RiseCusTotalInfo setRiseCusTotalInfo(ResultSet resultSet, String ump_mobile_no, String batchno) {
        RiseCusTotalInfo riseCusTotalInfo = new RiseCusTotalInfo();
        try {
        riseCusTotalInfo.setBatchno(batchno);
        riseCusTotalInfo.setUmp_mobile_no(ump_mobile_no);
        riseCusTotalInfo.setCustomer_no(resultSet.getString("CUSTOMER_NO"));
        riseCusTotalInfo.setMobile_no(resultSet.getString("MOBILE_NO"));
        riseCusTotalInfo.setMobile_chnl(resultSet.getString("MOBILE_CHNL"));
        riseCusTotalInfo.setMain_id_type(resultSet.getString("MAIN_ID_TYPE"));
        riseCusTotalInfo.setMain_id_no(resultSet.getString("MAIN_ID_NO"));
        riseCusTotalInfo.setCustomer_name(resultSet.getString("CUSTOMER_NAME"));
        riseCusTotalInfo.setCustomer_type(resultSet.getString("CUSTOMER_TYPE"));
        riseCusTotalInfo.setProv_br_no(resultSet.getString("PROV_BR_NO"));
        riseCusTotalInfo.setCus_branch_no(resultSet.getString("CUS_BRANCH_NO"));
        riseCusTotalInfo.setBr_name(resultSet.getString("BR_NAME"));
        riseCusTotalInfo.setCreate_brno(resultSet.getString("CREATE_BRNO"));
        riseCusTotalInfo.setSecond_branch_no(resultSet.getString("SECOND_BRANCH_NO"));
        riseCusTotalInfo.setSecond_br_name(resultSet.getString("SECOND_BR_NAME"));
        riseCusTotalInfo.setAddress(resultSet.getString("ADDRESS"));
        riseCusTotalInfo.setPhone_no_off(resultSet.getString("PHONE_NO_OFF"));
        riseCusTotalInfo.setPhone_no_house(resultSet.getString("PHONE_NO_HOUSE"));
        riseCusTotalInfo.setNationality_cd(resultSet.getString("NATIONALITY_CD"));
        riseCusTotalInfo.setTitle_code(resultSet.getString("TITLE_CODE"));
        riseCusTotalInfo.setMarital_status(resultSet.getString("MARITAL_STATUS"));
        riseCusTotalInfo.setPosition(resultSet.getString("POSITION"));
        riseCusTotalInfo.setOccupation(resultSet.getString("OCCUPATION"));
        riseCusTotalInfo.setBirthday(resultSet.getString("BIRTHDAY"));
        riseCusTotalInfo.setAge(resultSet.getString("AGE"));
        riseCusTotalInfo.setEmployer(resultSet.getString("EMPLOYER"));
        riseCusTotalInfo.setEmpl_addr(resultSet.getString("EMPL_ADDR"));
        riseCusTotalInfo.setRisk_grade(resultSet.getString("RISK_GRADE"));
        riseCusTotalInfo.setCus_level(resultSet.getString("CUS_LEVEL"));
        riseCusTotalInfo.setPt_card_flg(resultSet.getString("PT_CARD_FLG"));
        riseCusTotalInfo.setPhone_bank_flg(resultSet.getString("PHONE_BANK_FLG"));
        riseCusTotalInfo.setBusiness_flg(resultSet.getString("BUSINESS_FLG"));
        riseCusTotalInfo.setMobile_bank_flg(resultSet.getString("MOBILE_BANK_FLG"));
        riseCusTotalInfo.setDeposit_flag(resultSet.getString("DEPOSIT_FLAG"));
        riseCusTotalInfo.setNet_bank_flg(resultSet.getString("NET_BANK_FLG"));
        riseCusTotalInfo.setCard_flg(resultSet.getString("CARD_FLG"));
        riseCusTotalInfo.setMobile_msg_flag(resultSet.getString("MOBILE_MSG_FLAG"));
        riseCusTotalInfo.setCredit_card_flg(resultSet.getString("CREDIT_CARD_FLG"));
        riseCusTotalInfo.setXpad_acct_cny_flag(resultSet.getString("XPAD_ACCT_CNY_FLAG"));
        riseCusTotalInfo.setXpad_acct_other_flag(resultSet.getString("XPAD_ACCT_OTHER_FLAG"));
        riseCusTotalInfo.setTpcc_flag(resultSet.getString("TPCC_FLAG"));
        riseCusTotalInfo.setFund_flag(resultSet.getString("FUND_FLAG"));
        riseCusTotalInfo.setFnacno_flag(resultSet.getString("FNACNO_FLAG"));
        riseCusTotalInfo.setMetal_flag(resultSet.getString("METAL_FLAG"));
        riseCusTotalInfo.setMetal_flag(resultSet.getString("METAL_FLAG"));
        riseCusTotalInfo.setDeposit_amt(resultSet.getString("DEPOSIT_AMT"));
        riseCusTotalInfo.setLoan_amt(resultSet.getString("LOAN_AMT"));
        riseCusTotalInfo.setFund_amt(resultSet.getString("FUND_AMT"));
        riseCusTotalInfo.setBond_amt(resultSet.getString("BOND_AMT"));
        riseCusTotalInfo.setIbas_amt(resultSet.getString("IBAS_AMT"));
        riseCusTotalInfo.setGold(resultSet.getString("GOLD"));
        riseCusTotalInfo.setTpcc_amt(resultSet.getString("TPCC_AMT"));
        riseCusTotalInfo.setFinance_in(resultSet.getString("FINANCE_IN"));
        riseCusTotalInfo.setFinance_out(resultSet.getString("FINANCE_OUT"));
        riseCusTotalInfo.setFinance_other(resultSet.getString("FINANCE_OTHER"));
        riseCusTotalInfo.setMetal_amt(resultSet.getString("METAL_AMT"));
        riseCusTotalInfo.setCard_amt(resultSet.getString("CARD_AMT"));
        riseCusTotalInfo.setAum_amt(resultSet.getString("AUM_AMT"));
        riseCusTotalInfo.setCurrency_sum(resultSet.getString("CURRENCY_SUM"));
        riseCusTotalInfo.setThre_mon_c_tran_amount(resultSet.getString("THRE_MON_C_TRAN_AMOUNT"));
        riseCusTotalInfo.setSix_mon_c_tran_amount(resultSet.getString("SIX_MON_C_TRAN_AMOUNT"));
        riseCusTotalInfo.setTwlv_mon_c_tran_amount(resultSet.getString("TWLV_MON_C_TRAN_AMOUNT"));
        riseCusTotalInfo.setThre_mon_d_tran_amount(resultSet.getString("THRE_MON_D_TRAN_AMOUNT"));
        riseCusTotalInfo.setSix_mon_d_tran_amount(resultSet.getString("SIX_MON_D_TRAN_AMOUNT"));
        riseCusTotalInfo.setTwlv_mon_d_tran_amount(resultSet.getString("TWLV_MON_D_TRAN_AMOUNT"));
        riseCusTotalInfo.setThre_mon_fina_peak(resultSet.getString("THRE_MON_FINA_PEAK"));
        riseCusTotalInfo.setSix_mon_fina_peak(resultSet.getString("SIX_MON_FINA_PEAK"));
        riseCusTotalInfo.setTwlv_mon_fina_peak(resultSet.getString("TWLV_MON_FINA_PEAK"));
        riseCusTotalInfo.setYear_avg_fina_assets(resultSet.getString("YEAR_AVG_FINA_ASSETS"));
    }
        catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }
        return riseCusTotalInfo;
    }
}


