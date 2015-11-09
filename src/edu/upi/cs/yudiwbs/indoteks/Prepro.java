package edu.upi.cs.yudiwbs.indoteks;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *   Created by yudiwbs on 11/7/2015.
 *
 *   prepropcessing
 *
 *   casefolding, stopwords removal
 *

 table out:
 CREATE TABLE IF NOT EXISTS `articles_prepro` (
  id int(10) NOT NULL primary key,
  teks_prepro text
 )

 stopwords:
 CREATE TABLE IF NOT EXISTS `stopwords` (
 `id` int(10) NOT NULL AUTO_INCREMENT,
 `kata` varchar(100) DEFAULT NULL,
 PRIMARY KEY (`id`),
 UNIQUE KEY `kata` (`kata`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

 isistopword menggunakan method loadStopWords
 */

public class Prepro {

    private static final Logger logger = Logger.getLogger("prepro");

    public String dbName;
    public String userName;
    public String password;
    public String namaTableOut = "";           //table output
    public String namaTableIn  = "";           //table input
    public String namaFieldTeksIn;             //field teks
    public String namaFieldIdIn="id";          //field id
    public String namaFieldIdOut="id";         //field id output, isinya akan disamakan dengan id input
    public String namaFieldTeksPreproOut="";   //field tempat nilai tfidf disimpan

    ArrayList<String> alStopWords = new  ArrayList<String>();

    private void loadStopWords(String namaTabel,String namaField) {
        //memindahkan data stopwords dari tabel ke memori alStopWords
        System.out.println("loadStopWords");
        Connection conn=null;
        PreparedStatement pSel=null;
        alStopWords.clear();
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://"+dbName+"?user="+userName+"&password="+password);
            pSel  = conn.prepareStatement (String.format("select id,%s from %s",namaField,namaTabel));
            ResultSet rs = pSel.executeQuery();
            int jumDiproses = 0;
            while (rs.next())  {
                String kata = rs.getString(2).trim();
                alStopWords.add(kata);
                //System.out.println(kata);
                jumDiproses++;
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, null, e);
        }
        finally  {
            try  {
                if (pSel!= null) {pSel.close();}
                if (conn != null) {conn.close();}
            } catch (Exception e) {
                logger.log(Level.SEVERE, null, e);
            }
        }
    }

    public void fileStopwordsToDB(String fileName,String tableName,String fieldName) {
        //utility memindahkan isi file teks ke tabel
        //berguna untuk menambahkan data stopwords baru
        //melakukan pengecekan, kalau ada duplikasi maka tidak dimasukkan, jadi tidak perlu dihapus sebelumnya


        System.out.println("filetodbstopwords");
        Connection conn=null;
        PreparedStatement pSdhAda=null;
        PreparedStatement pIns=null;
        int jumTdkDiproses=0;
        int jumDiproses=0;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection  ("jdbc:mysql://"+dbName+"?user="+userName+"&password="+password);
            pSdhAda = conn.prepareStatement     (" select id from  "+ tableName + " where "+ fieldName +" = ?");
            pIns    =  conn.prepareStatement    (" insert into  "+ tableName + "("+fieldName+") values (?)");

            FileInputStream fstream = new FileInputStream(fileName);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            ResultSet rs = null;
            while ((strLine = br.readLine()) != null)   {
                if (strLine.equals("")) {continue;}
                //masuk ke tabel?
                pSdhAda.setString(1,strLine);
                rs = pSdhAda.executeQuery();
                if (rs.next()) {
                    //sudah ada, batalkan masuk
                    jumTdkDiproses++;
                } else {
                    jumDiproses++;
                    pIns.setString(1,strLine);
                    pIns.executeUpdate();
                }
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, null, e);
        }
        finally  {
            try  {
                if (pSdhAda != null) {pSdhAda.close();}
                if (pIns != null)    {pIns.close();}
                if (conn != null) {conn.close();}
            } catch (Exception e) {
                logger.log(Level.SEVERE, null, e);
            }
        }
        System.out.println("selesai");
    }




    private String preproDasar(String strIn) {
        //loadstopwords sudah dipanggil

        String strOut;
        strOut = strIn.toLowerCase();        //casefolding
        //strOut = strOut.replaceAll("[0-9()\"\\-.,]"," ");
        strOut = strOut.replaceAll("[[^a-z ][\\-]]"," ");
        //proses stopwords
        Scanner sc = new Scanner(strOut);
        StringBuilder sb = new StringBuilder();
        while (sc.hasNext()) {
            String kata = sc.next();
            if (alStopWords.contains(kata)) {
                continue;
            }
            sb.append(kata);
            sb.append(" ");
        }
        sc.close();
        strOut = sb.toString();
        return strOut;
    }

    public void proses() {
        //harus dibersihkan dulu tabelnya manual
        loadStopWords("stopwords","kata");

        System.out.println("Mulai proses prepro");
        Connection conn=null;
        PreparedStatement pSel = null;
        PreparedStatement pIns = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://"+dbName+"?user="+userName+"&password="+password);
            conn.setAutoCommit(false);
            pSel  =  conn.prepareStatement (String.format(" select %s, %s from  %s ",namaFieldIdIn,namaFieldTeksIn,namaTableIn));
            pIns  =  conn.prepareStatement (String.format(" insert into %s(%s,%s) values (?,?)",namaTableOut,namaFieldIdOut,namaFieldTeksPreproOut));

            ResultSet rsSel = pSel.executeQuery();
            int jumDiproses = 0;
            while ( rsSel.next())  {
                long idInternal = rsSel.getLong(1);
                String doc = rsSel.getString(2);
                jumDiproses++;
                //proses disini
                String strOut = preproDasar(doc);
                //System.out.println(strOut);
                pIns.setLong(1,idInternal);
                pIns.setString(2,strOut);
                pIns.executeUpdate();
            }
            // System.out.println("-------");
            System.out.println("selesai");
        }
        catch (Exception e) {
            //ROLLBACK
            logger.log(Level.SEVERE, null, e);
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    logger.log(Level.SEVERE, null, e1);
                }
                System.out.println("Connection rollback...");
            }
        }
        finally  {
            try  {
                conn.commit();
                if (pSel != null) {pSel.close();}
                if (pIns != null) {pIns.close();}
                if (conn != null) {conn.close();}
            } catch (Exception e) {
                logger.log(Level.SEVERE, null, e);
            }
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
                pDelete.close();
                conn.close();
            } catch (Exception e) {
                logger.log(Level.SEVERE, null, e);
            }
        }

    }

    public static void main(String[] args) {
        String s = "Bandung (ANTARA News) - Tim \"Mutiara Hitam\" Persipura Jayapura mengalahkan tuan rumah Pelita Bandung Raya (PBR) 2-0 pada lanjutan Liga Super Indonesia (LSI) 2013 di Stadion Si Jalak Harupat Soreang Kabupaten Bandung, Minggu.\n" +
                "Gol kemenangan tim Jayapura itu diborong kapten tim Boas Salosa masing-masing melalui penalti menit ke-9 dan tembakan kaki kiri pada menit ke-58.\n" +
                "Dengan kemenangan itu, Persipura kembali menempati puncak klasemen sementara Liga Super Indonesia 2013 menggeser Mitra Kukar. Persipura mengantongi total nilai 24, hasil 14 kali main dengan 10 kali menang dan empat seri  tanpa kalah.\n" +
                "Persipura juga menjadi tim paling produktif mencetak 31 gol dan hanya kemasukan empat gol. Selain itu, dua gol Boaz Salosa menjadikannya kokoh menjadi top scorer Liga Super Indonesia 2013 dengan 12 gol, meninggalkan beberapa pesaingnya di deretan pencetak gol terbanyak.\n" +
                "Sebaliknya, bagi Pelita Bandung Raya, kekalahan itu membuatnya tetap berkutat di peringkat ke-16 klasemen dengan skor 11 hasil 14 kali berlaga, dua kali menang, lima seri dan tujuh kali kalah.\n" +
                "Kekalahan itu sekaligus juga merupakan kekalahan kandang kedua karena pada laga kandang sebelumnya pada Maret lalu, tim Bandung itu kalah dalam laga derby lawan Persib Bandung.";

        Prepro p = new Prepro();

        p.dbName="localhost/news";
        p.userName="news";
        p.password="news";
        p.namaTableOut = "articles_prepro";         //table output
        p.namaTableIn  = "articles";                //table input
        p.namaFieldTeksIn ="text";                 //field teks
        p.namaFieldIdIn="id";                     //field id
        p.namaFieldIdOut="id";                    //field id output, isinya akan disamakan dengan id input
        p.namaFieldTeksPreproOut="teks_prepro";   //field tempat nilai tfidf disimpan

        p.clearTableOut();
        p.proses();



        //p.fileStopwordsToDB("C:\\yudiwbs\\eksperimen\\news_aggregator\\stopwords.txt","stopwords","kata");

        //debug String out = p.preproDasar(s);
        //System.out.println(out);
    }


}
