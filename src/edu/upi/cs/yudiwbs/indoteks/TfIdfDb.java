package edu.upi.cs.yudiwbs.indoteks;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by yudiwbs on 11/7/2015.
 *
 * contoh output table:
 * create table out (id int  not null key auto_increment primary key, tfidf text);
 *
 */

public class TfIdfDb {
    private static final Logger logger = Logger.getLogger("TfidfDb");
    private static final int MINFREQ  = 3;  //minimum kemunculam tweet yg mengandung term supaya dihitung


    //baca dari DB, hitung tf-idf, tulis ke DB
    public String dbName;
    public String userName;
    public String password;
    public String namaTableOut = "";     //table output
    public String namaTableIn  = "";    //table input
    public String namaFieldTeksIn;             //field teks
    public String namaFieldIdIn="id";          //field id
    public String namaFieldIdOut="id";         //field id output, isinya disamakan dengan id
    public String namaFieldTfIdfOut="tfidf";      //field tempat nilai tfidf disimpan

    //private ArrayList<String>  alExtStopWords = new ArrayList<String>();

    private class TermStatComparable implements Comparator<TermStat> {
        @Override
        public int compare(TermStat o1, TermStat o2) {
            return (o1.getAvg()>o2.getAvg() ? -1 : (o1.getAvg()==o2.getAvg() ? 0 : 1));
        }
    }

    public void clearTableOut() {
        //hapus semua isi tableOut
        //idealnya nanti diganti dengan proses incremental (online),
        //tapi untuk sekarang dihapus dulu lalu diisi dengan yang baru
        Connection conn=null;
        PreparedStatement pDelete = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String strCon = "jdbc:mysql://"+dbName+"?user="+userName+"&password="+password;
            conn = DriverManager.getConnection(strCon);
            conn.setAutoCommit(false);
            String SQLdelete = "delete from "+namaTableOut;
            pDelete  =  conn.prepareStatement (SQLdelete);
            pDelete.executeUpdate();
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
            logger.severe(e.toString());
            logger.severe(e.toString()); //ROLLBACK
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
                if (pDelete != null) {
                    pDelete.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, null, e);
            }
        }

    }

    /**
     * Menghitung rawtfidf
     *
     *

     *
     *  output adalah tabel berisi pasangan kata dan bobot rawtfidfnya
     *  hitung term freq tf(i,j): frekuensi term i di doc  j / jumlah kata dalam doc tsb
     *  hitung idf(i): log (jumlah semua tweet dalam corpus / jumlah tweet yg mengandung term i)
     *  hitung tf-idf(i,j) = tf(i,j) * idf (i)
     *
     *  table untuk menampung output tfidif sudah dikosongkan  (TBD: perlu dikosongkan terlebih dulu??)
     *  field yang dipreoses adalah
     *
     */

    public void proses(String strFilter) {

        logger.info("mulai");
        Connection conn=null;
        PreparedStatement pTw = null;
        PreparedStatement pInsertTfIdf = null;
        String kata;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String strCon = "jdbc:mysql://"+dbName+"?user="+userName+"&password="+password;
            System.out.println(strCon);
            conn = DriverManager.getConnection(strCon);
            conn.setAutoCommit(false);


            int cc=0;

            HashMap<String,Integer> tweetsHaveTermCount  = new HashMap<>();      //jumlah doc yg mengandung sebuah term
            ArrayList<HashMap<String,Integer>> arrTermCount = new ArrayList<>(); //freq kata untuk setiap doc
            ArrayList<Long>  arrIdInternalTw = new ArrayList<>(); //untuk menyimpan id

            Integer freq;

            //String SQLambilTw = "select id_internal,text_prepro from "+tableIn+ " where is_prepro=1 "+filter;
            String SQLambilTw = String.format("select %s,%s from %s %s",
                    namaFieldIdIn,namaFieldTeksIn,namaTableIn,strFilter);
            System.out.println(SQLambilTw);
            pTw  =  conn.prepareStatement (SQLambilTw);


            //pInsertTfIdf =
                    //conn.prepareStatement("insert into "+ tableTfidf + " (id_internal_tw_jadi,tfidf_val)"
                    //        + "values (?,?) ");

            String sqlInsert = String.format("insert into %s (%s,%s) values (?,?)",
                    namaTableOut,namaFieldIdOut,namaFieldTfIdfOut);
            pInsertTfIdf = conn.prepareStatement(sqlInsert);

            ResultSet rsTw = pTw.executeQuery();
            while (rsTw.next())   {                           //loop untuk setiap doc
                long idInternalTw = rsTw.getLong(1);
                arrIdInternalTw.add(idInternalTw);
                String tw = rsTw.getString(2);
                HashMap<String,Integer> termCount  = new HashMap<>(); //freq term dalam satu tweet
                cc++;
                System.out.println(cc);
                Scanner sc = new Scanner(tw);
                while (sc.hasNext()) {
                    kata = sc.next();
                    freq = termCount.get(kata);  //ambil kata
                    //jika kata itu tidak ada, isi dengan 1, jika ada increment
                    termCount.put(kata, (freq == null) ? 1 : freq + 1);
                }
                sc.close();  //satu baris selesai diproses (satu tweet)
                arrTermCount.add(termCount);  //tambahkan

                //termCount berisi kata dan freq di sebuah tweet
                //berd termCount hitung jumlah tweet yg mengandung sebuah term
                for (String term : termCount.keySet()) {
                    //jika kata itu tidak ada, isi dengan 1, jika ada increment
                    freq = tweetsHaveTermCount.get(term);  //ambil kata
                    tweetsHaveTermCount.put(term, (freq == null) ? 1 : freq + 1);
                }
            }  //while

            double numOfTweets = cc;

            // hitung idf(i) = log (NumofTw / countTwHasTerm(i))
            HashMap<String,Double> idf = new HashMap<>();
            double jumTweet;
            for (Map.Entry<String,Integer> entry : tweetsHaveTermCount.entrySet()) {
                //System.out.println(entry.getKey()+"="+entry.getValue());
                //modif, untuk term hanys satu kali muncul set idf dengan 0, sebagai mark agar term tersebut dihapus
                jumTweet = entry.getValue();
                String key = entry.getKey();
                idf.put(key, Math.log(numOfTweets/jumTweet));
            }

            //hitung tfidf, tf yg digunakan tidak dibagi dengan jumlah kata di dalam tweet karena diasumsikan relatif sama
            double tfidf;cc=0;

            //for (HashMap<String,Integer> hm : arrTermCount) {   //untuk semua tweets
            for (int i=0;i<arrTermCount.size();i++) {
                HashMap<String,Integer> hm = arrTermCount.get(i);
                Long idInternalTw = arrIdInternalTw.get(i);
                cc++;
                //System.out.println(cc+":");
                double idfVal;
                String key;
                StringBuilder sb = new StringBuilder();
                for (Map.Entry<String,Integer> entry : hm.entrySet()) {  //untuk term dalam satu tweet
                    key =entry.getKey();
                    idfVal = idf.get(key);
                    if (idfVal>=0) {   //kalau < 0 artinya diskip karena jumlah tweet yg mengandung term tersbut terlalu sedikit
                        tfidf  = entry.getValue() * idfVal ;     //rawtf * idf
                        sb.append(entry.getKey()); //jangan pake ;
                        sb.append("==>");
                        sb.append(tfidf);
                        sb.append(";;");
                    }
                }
                pInsertTfIdf.setLong(1, idInternalTw);
                pInsertTfIdf.setString(2, sb.toString());
                pInsertTfIdf.addBatch();
            }
            pInsertTfIdf.executeBatch();
            System.out.println("selesai");
        } catch (Exception e) {
            e.printStackTrace();
            logger.severe(e.toString()); //ROLLBACK
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
                if (pInsertTfIdf != null) {
                    pInsertTfIdf.close();
                }
                if (pTw != null) {
                    pTw.close();
                }
                if (conn != null) {
                    conn.commit();
                    conn.setAutoCommit(true);
                    conn.close();
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, null, e);
            }
        }
    }

    public void stat(String namaFileOutput,boolean hanyaKata) {
        //menggunakan namaTabelOut dan namaFieldIdOut, namaFieldTfIdfOut
        //hanyaKata = true => kata saja yang ditampilkan tanpa skor dan freq (enak untuk buat daftar stopwords)

        /*input: tabel tfidf corpus (hasil dari method proses(). Contoh isi field:
           di=2.1972245773362196|ayo=5.123963979403259|cinta=5.198497031265826|

        //output: rata-rata bobot tfidf untuk semua kata yg terurut dari besar ke kecil
        //bisa digunakan untuk menentukan kata yang akan masuk ke stopwords

          namaFileOut lengkap dengan path
        */
        Connection conn = null;
        PreparedStatement pSel = null;
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(namaFileOutput);
            String strLine;
            int cc = 0;
            HashMap<String, TermStat> termMap = new HashMap<>();
            String[] str;
            TermStat ts;

            String sql = String.format("select %s,%s from %s", namaFieldIdOut, namaFieldTfIdfOut, namaTableOut);
            Class.forName("com.mysql.jdbc.Driver");
            String strCon = "jdbc:mysql://" + dbName + "?user=" + userName + "&password=" + password;
            conn = DriverManager.getConnection(strCon);
            pSel = conn.prepareStatement(sql);

            ResultSet rsTw = pSel.executeQuery();
            while (rsTw.next()) {
                cc++;
                System.out.println(cc);
                //di=2.1972245773362196|ayo=5.123963979403259|cinta=5.198497031265826;
                strLine = rsTw.getString(2);
                System.out.println(strLine);//debug
                Scanner sc = new Scanner(strLine);
                sc.useDelimiter(";;");

                while (sc.hasNext()) {
                    String item = sc.next(); //pasangan term=val
                    System.out.println(cc+":"+item);//debug
                    str = item.split("==>");
                    ts = termMap.get(str[0]);
                    double val = Double.parseDouble(str[1]);
                    if (ts == null) {
                        termMap.put(str[0], new TermStat(str[0], val));
                    } else {
                        //ts.incFreq();
                        ts.addVal(val);
                    }
                }

            } //semua baris sudah dibaca
            ArrayList<TermStat> arrTS = new ArrayList<>();
            for (Map.Entry<String, TermStat> term : termMap.entrySet()) {
                ts = term.getValue();
                if (ts.getFreq() < MINFREQ) {
                    continue;
                } //skip term yg hanya muncul xx kali
                ts.calcAvg();
                arrTS.add(ts);
            }
            Collections.sort(arrTS, new TermStatComparable());
            if (hanyaKata) {
                for (TermStat t : arrTS) {
                    pw.println(t.term);
                }
            } else {
                for (TermStat t : arrTS) {
                    pw.println(t.term + "=" + t.getAvg() + "; freq=" + t.getFreq());
                }
            }

        } catch (SQLException | FileNotFoundException | ClassNotFoundException e) {
            logger.severe(e.toString());
        } finally {
            try {
                if (pSel != null) {
                    pSel.close();
                }
                if (pw != null) {
                    pw.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, null, e);
            }
        }
    }

    public static void main(String[] Args) {
        //testing
        TfIdfDb t = new TfIdfDb();
        t.dbName="localhost/news";
        t.userName="news";
        t.password="news";
        t.namaTableIn="articles_prepro";
        t.namaFieldIdIn="id";
        t.namaFieldTeksIn="teks_prepro";
        t.namaTableOut="tfidf_articles";
        t.namaFieldIdOut="id";
        t.namaFieldTfIdfOut="tfidf";

        //t.clearTableOut();
        //t.proses("");

        t.stat("C:\\yudiwbs\\eksperimen\\news_aggregator\\stat_tfidf.txt",false);

        //t.process("g:\\eksperimen\\data_weka\\tw_obama.txt","g:\\eksperimen\\data_weka\\stopwords.txt");
        //t.process("e:\\tweetmining\\corpus_0_1000_prepro_nots_preproSyn.txt","e:\\tweetmining\\catatan_stopwords_weight.txt");
    }

}
