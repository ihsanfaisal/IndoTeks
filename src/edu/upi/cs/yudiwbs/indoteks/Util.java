package edu.upi.cs.yudiwbs.indoteks;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by yudiwbs on 11/8/2015.
 *
 * proses yg terkait dengan kasus2, tidak masuk ke dalam library
 * jadi akan banyak variabel2 yg hardcode
 *
 *
 */


public class Util {
    private static final Logger logger = Logger.getLogger("util");
    public String dbName;
    public String userName;
    public String password;




    public void multitosingle(String namaTabelIn,String namaTabelOut) {

        String sqlSel = " select id,`class-Pendidikan`,`class-Politik`,`class-HukumKriminal`," +
                "`class-SosialBudaya`,`class-Olahraga`,`class-TeknologiSains`,`class-Hiburan`," +
                "`class-EkonomiBisnis`,`class-Kesehatan`,`class-BencanaKecelakaan` from "+namaTabelIn;

        System.out.println(sqlSel);

        String sqlUpdate = "update "+ namaTabelOut +" set kelas = ? where id = ?";



        //ambil dari data articles, pilih kelas, isi ke tabel articles_prepro.kelas
        //kalau lebih dari satu, ambil yg terakhir
        logger.info("mulai");
        Connection conn=null;
        PreparedStatement pSel    = null;
        PreparedStatement pUpdate = null;
        long id;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String strCon = "jdbc:mysql://"+dbName+"?user="+userName+"&password="+password;
            System.out.println(strCon);
            conn = DriverManager.getConnection(strCon);
            conn.setAutoCommit(false);
            pSel    =  conn.prepareStatement (sqlSel);
            pUpdate = conn.prepareStatement(sqlUpdate);
            ResultSet rsSel = pSel.executeQuery();
            while (rsSel.next())   {
                //efek tabel nggak dinormaliasai :(
                id = rsSel.getLong(1);
                System.out.println(id);
                boolean pendidikan = rsSel.getBoolean(2);
                boolean politik = rsSel.getBoolean(3);
                boolean hukumKriminal = rsSel.getBoolean(4);
                boolean sosialBudaya  = rsSel.getBoolean(5);
                boolean olahraga = rsSel.getBoolean(6);
                boolean teknologiSains = rsSel.getBoolean(7);
                boolean hiburan = rsSel.getBoolean(8);
                boolean ekonomiBisnis = rsSel.getBoolean(9);
                boolean kesehatan = rsSel.getBoolean(10);
                boolean bencanaKecelakaan = rsSel.getBoolean(11);

                int kelas = -1;
                //tidak menggunakan else karena memang bisa lebih dari satu
                //untuk yg sekarang, diambil kelas yang paling akhir
                if (pendidikan) {
                    kelas = 1;
                }
                if (politik) {
                    kelas = 2;
                }
                if (hukumKriminal) {
                    kelas = 3;
                }
                if (sosialBudaya) {
                    kelas = 4;
                }
                if (olahraga) {
                    kelas = 5;
                }
                if (teknologiSains) {
                    kelas = 6;
                }
                if (hiburan)  {
                    kelas = 7;
                }
                if (ekonomiBisnis) {
                    kelas = 8;
                }
                if (kesehatan) {
                    kelas = 9;
                }
                if (bencanaKecelakaan) {
                    kelas = 10;
                }
                //debug
                System.out.println(id+":"+kelas);
                pUpdate.setInt(1,kelas);  //kelas
                pUpdate.setLong(2,id);    //id
                pUpdate.executeUpdate();
                conn.commit();
            }
            } catch (Exception e) {
                e.printStackTrace();
                logger.severe(e.toString());
                if (conn != null) {
                    try {
                        conn.rollback();
                    } catch (SQLException e1) {
                        logger.log(Level.SEVERE, null, e1);
                    }
                    System.out.println("Connection rollback...");
                }
            }
        finally {
            try {
                if (conn != null) {
                    conn.commit();
                }
                if (pSel != null) {
                    pSel.close();
                }
                if (pUpdate != null) {
                    pUpdate.close();
                }
                if (conn != null) {
                    conn.close();
                }
                System.out.println("selesai");
            } catch (Exception e) {
                logger.log(Level.SEVERE, null, e);
            }
        }


    }


    public static void main(String[] args) {
        Util t = new Util();
        t.dbName="localhost/news";
        t.userName="news";
        t.password="news";
        System.out.println("hoi");
        t.multitosingle("datatest","datatest_prepro");
    }

}
