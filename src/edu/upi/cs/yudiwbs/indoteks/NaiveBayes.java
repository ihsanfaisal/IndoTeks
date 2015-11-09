package edu.upi.cs.yudiwbs.indoteks;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *   Created by yudiwbs on 11/8/2015.
 *   Lisensi: LGPL
 *
 *

 //menyimpan hasil probabilias
 CREATE TABLE IF NOT EXISTS modelnb_prob_kelas (
 id          bigint(20) NOT NULL AUTO_INCREMENT primary key,
 id_kelas    bigint(20) NOT NULL,
 prob_class  double,
 jumkata_class int,
 prob_freq_nol double,
 KEY `id_kelas` (`id_kelas`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

 CREATE TABLE IF NOT EXISTS modelnb_prob_kata (
 id          bigint(20) NOT NULL AUTO_INCREMENT primary key,
 id_kelas    bigint(20) NOT NULL,
 kata        varchar(60),
 prob        double,
 KEY `id_kelas` (`id_kelas`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8;


 //informasi tentang kelas
 CREATE TABLE IF NOT EXISTS `kelas` (
 `id_internal` bigint(20) NOT NULL AUTO_INCREMENT,
 `id_kelas`    bigint(20) NOT NULL,
 `nama_kelas` varchar(50) NOT NULL,
 `desc` varchar(75) NOT NULL,
 PRIMARY KEY (`id_internal`),
 KEY `id_kelas` (`id_kelas`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8;



 untuk test model (cross validation), tambahkan 2 field di tabel yang akan diproses:
 ALTER TABLE articles_prepro ADD COLUMN `partisi` INT NULL DEFAULT '0' AFTER `sanggahan_dukungan`;
 ALTER TABLE articles_prepro ADD INDEX `partisi` (`partisi`);

 */

public class NaiveBayes {

    //baca dari DB
    public String dbName;
    public String userName;
    public String password;
    public String namaTableIn  = "";    //table untuk learning
    public String namaFieldClass;       //field class
    public String namaFieldIdIn="id";   //id
    public String namaFieldTeksIn="";   //teks



    private final Logger logger = Logger.getLogger("naive bayes DB");

    //todo dipindahkan karena pasti nanti banyak digunakan di class lain
    public void clearTable(String namaTabel) {
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
            String SQLdelete = "delete from "+namaTabel;
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

    //prob kelas hasil learning
    public class ProbKelas {
        int idKelas;
        String namaKelas;
        int jumDoc;      //jumlah doc dalam kelas tersebut
        double probKelas;
        double probDef;    //probabilitas default untuk kata yang freq=0
        HashMap<String,Integer> countWord;  //count word per class
        HashMap<String,Double> probWord = new HashMap<String,Double>();    //prob word per class

        public void print() {
            //untuk kepentingan debug
            System.out.println("id kelas="+idKelas);
            System.out.println("nama kelas="+namaKelas);
            System.out.println("jumdoc="+jumDoc);
            System.out.println("probKelas="+probKelas);
            System.out.println("probDef="+probDef);

            //print count word
            System.out.println("count word");
            for (Map.Entry<String,Integer> entry : countWord.entrySet())  {
                String kata = entry.getKey();
                Integer freq = entry.getValue();
                System.out.println(kata+":"+freq);
            }

            //print prob word
            System.out.println("prob word");
            for (Map.Entry<String,Double> entry : probWord.entrySet())  {
                String kata = entry.getKey();
                Double prob = entry.getValue();
                System.out.println(kata+":"+prob);
            }
        }


    }

    private ArrayList<ProbKelas> learn(String tambahanFilter) {
        //menghitung probablitas kelas (lihat class ProbKelas)

        //tambahan filter berguna misalnya untuk memproses 10 fold cross validation
        //contoh --> learn("and partisi<>1")   maka hanya partisi<>1 yang diproses utk dijadikan model
        //harus diawali dengan "and"

        //input tabel kelas (id_internal,id_kelas,nama_kelas,desc)
		/*

		CREATE TABLE IF NOT EXISTS `kelas` (
				  `id_internal` bigint(20) NOT NULL AUTO_INCREMENT,
				  `id_kelas`    bigint(20) NOT NULL,
				  `nama_kelas` varchar(50) NOT NULL,
				  `desc` varchar(75) NOT NULL,
				  PRIMARY KEY (`id_internal`),
				  KEY `id_kelas` (`id_kelas`)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8;
		*/

        Logger log = Logger.getLogger("naive bayes DB learning ");
		System.out.println("Naive Bayes: Learning");

        //ambil data, pindahkan ke memori

        //  hitung freq kemunculan setiap kata
        //  hitung jumlah total kata
        //  hitung jumlah doc

        int jumDocTotal  = 0;
        int jumWordTotal   = 0;  //jumlah distinct kata (total kosakata)
        ArrayList<ProbKelas> alProbKelas = new ArrayList<ProbKelas>();

        String strSQLJumKelas        = "select count(*) from kelas";
        String strSQLAmbilIdKelas    = "select id_internal,id_kelas,nama_kelas from kelas";


        //String strSQLAmbilTw    = "select id_internal,text_prepro from tw_jadi where "+fieldClass+" = ? "+tambahanFilter;

        String strSQLAmbilTw      =  String.format("select %s,%s from %s where %s  = ? ",
                namaFieldIdIn,namaFieldTeksIn,namaTableIn,namaFieldClass)   +tambahanFilter;

        System.out.println(strSQLAmbilTw);

        Connection conn=null;

        PreparedStatement pJumKelas=null;
        PreparedStatement pAmbilIdKelas=null;
        PreparedStatement pAmbilDoc = null;

        ResultSet rsJumKelas=null;
        ResultSet rsAmbilIdKelas=null;
        ResultSet rsTw=null;



        int jumKelas=-1;
        try  {
            Class.forName("com.mysql.jdbc.Driver");
            String strCon = "jdbc:mysql://"+dbName+"?user="+userName+"&password="+password;
            //ambil id_class
            conn = DriverManager.getConnection(strCon);
            pJumKelas  =  conn.prepareStatement (strSQLJumKelas);
            rsJumKelas =  pJumKelas.executeQuery();
            if (rsJumKelas.next()) {
                jumKelas = rsJumKelas.getInt(1);
                //System.out.println("Jumlah kelas:"+jumKelas);
            } else  {
                System.out.println("Tidak bisa mengakses tabel kelas, abort");
                System.exit(1);
            }

            //int[] jumTweetPerClass = new int[jumKelas];

            pAmbilIdKelas   = conn.prepareStatement(strSQLAmbilIdKelas);
            rsAmbilIdKelas  = pAmbilIdKelas.executeQuery();
            pAmbilDoc       = conn.prepareStatement(strSQLAmbilTw);


            while (rsAmbilIdKelas.next()) {  //loop untuk setiap kelas
                ProbKelas pk = new ProbKelas();
                HashMap<String,Integer> countWord  = new HashMap<String,Integer>();
                pk.countWord=countWord;

                int idKelas = rsAmbilIdKelas.getInt(2);
                String namaKelas = rsAmbilIdKelas.getString(3);
                //System.out.println("Memproses kelas:"+namaKelas);
                pk.idKelas = idKelas;
                pk.namaKelas = namaKelas;

//        		 System.out.println("id kelas"+idKelas);
//        		 System.out.println("Nama kelas:"+namaKelas);


                //ambil semua docs untuk kelas tsb
                pAmbilDoc.setInt(1, idKelas);
                rsTw = pAmbilDoc.executeQuery();

                int cc=0;
                while (rsTw.next()) {  //untuk setiap doc
                    cc++;
                    String tw = rsTw.getString(2);  //ambil teks
                    //debug
                    //System.out.println("Proses:"+tw);
                    Scanner sc = new Scanner(tw);
                    //hitung frekuensi kata dalam doc
                    while (sc.hasNext()) {
                        String kata = sc.next();
                        Integer freq = countWord.get(kata);  //ambil kata
                        countWord.put(kata, (freq == null) ? 1 : freq + 1);      //jika kata itu tidak ada, isi dengan 1, jika ada increment
                    }
                    sc.close();
                }
                pk.jumDoc = cc;
                jumDocTotal  =  jumDocTotal  + cc;
                jumWordTotal =   jumWordTotal   + countWord.size();
                alProbKelas.add(pk);
            } //loop untuk setiap kelas

            //hitung probablitas
            //hitung p(vj) = jumlah doc kelas j / jumlah total doc
            //hitung p(wk | vj ) = (jumlah kata wk dalam kelas vj + 1 )  / jumlah total kata dalam kelas vj + |kosa kata|

            HashMap<String,Integer> countWord;
            double prob;
            int jumWord;

            for (ProbKelas pk:alProbKelas) {
                pk.probKelas = Math.log( (double) pk.jumDoc / jumDocTotal);
                countWord = pk.countWord;
                jumWord = countWord.size();  //jumlah kata di dalam kelas
                pk.probDef = Math.log((double) (1) / (jumWord + jumWordTotal)); //default value kata tanpa freq

                //prob untuk setiap kata dalam kelas tsb
                for (Map.Entry<String,Integer> entry : countWord.entrySet()) {
                    String kata = entry.getKey();
                    Integer freq = entry.getValue();
                    prob = Math.log((double) (freq + 1) / (jumWord + jumWordTotal ));  //dalam logiritmk
                    pk.probWord.put(kata, prob);
                }
            }

        }
        catch (Exception e)
        {
            //ROLLBACK
            logger.log(Level.SEVERE, null, e);
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    logger.log(Level.SEVERE, null, e1);
                }
                System.out.println("Fatal Error, rollback...");
                //isError = true;
            }
        }
        finally {
            try {
                pAmbilIdKelas.close();
                pAmbilDoc.close();
                rsJumKelas.close();
                rsAmbilIdKelas.close();
                rsTw.close();
                pJumKelas.close();
                conn.close();

            } catch (Exception e) {
                logger.log(Level.SEVERE, null, e);
                //isError = true;
            }
        }
        return alProbKelas;
    }

    public ProbKelas classifyMem(ArrayList<ProbKelas> model,String teks) {   //model diambil dari memori, cocok untuk x fold cross
        //System.out.println("klasifikasi tweet:"+tweet);
        double maxProb = -Double.MAX_VALUE;
        HashMap<String,Double> probWord;
        double totProb = 0;
        ProbKelas maxClass=null;

        //cari kelas dengan prob maksimal
        for (ProbKelas pk: model) {
            //System.out.println("kelas:"+pk.namaKelas);
            probWord =  pk.probWord; //  wordProbinClass.get(i);  //hashmap
            Scanner sc = new Scanner(teks);
            totProb = pk.probKelas;  //probClass[i];              //inisialisasi
            while (sc.hasNext()) {               //loop untuk semua kata
                String kata = sc.next();
                Double prob = probWord.get(kata);  //ambil probilitas kata
                if (prob!=null) {
                    //System.out.println("kata:"+kata+"; Prob="+prob);  //debug
                } else {  //kata tidak ada, menggunakan nilai standar
                    //System.out.println(" Menggunakan nilai def kata:"+kata+"; Prob="+pk.probDef);  //debug
                    prob = pk.probDef;
                }
                totProb = totProb + prob;
            } //while sc
            sc.close();
            //System.out.println("Total prob:"+totProb);
            if (totProb>maxProb)  {   //ambil yang paling besar
                //System.out.println("Tukar dengan maxprob:"+maxProb);
                maxProb = totProb;
                maxClass = pk;
            }
        }//for
        return maxClass;
    }

    public void learnToDB() {
		/*  Input:
		 *
		 *
		 *
		 *
		 *     Field yang digunakan:
		 *     - namaFieldClass adalah class (integer) dari record
		 * 	   - namaFieldTeksIn   berisi teks
		 *
		 *     Output ke DB
		 *
		 *     modelnb_prob_kelas     (id,id_kelas,prob_class,jumkata_class,prob_freq_nol)  --> nama kelas dan prob kelas
		 *     modelnb_prob_kata      (id,id_kelas,kata,prob)
		 *
		 *     create table
		      CREATE TABLE IF NOT EXISTS modelnb_prob_kelas (
				  id          bigint(20) NOT NULL AUTO_INCREMENT primary key,
				  id_kelas    bigint(20) NOT NULL,
				  prob_class  double,
				  jumkata_class int,
				  prob_freq_nol int,
				  KEY `id_kelas` (`id_kelas`)
			  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;


			  CREATE TABLE IF NOT EXISTS modelnb_prob_kata (
				  id          bigint(20) NOT NULL AUTO_INCREMENT primary key,
				  id_kelas    bigint(20) NOT NULL,
				  prob        double,
				  KEY `id_kelas` (`id_kelas`)
			  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;


		 *
		 */


        Logger log = Logger.getLogger("naive bayes DB learning DB");

        //String[] className = new String[inputFile.length];


        System.out.println("Naive Bayes: Learning & tulis ke DB");
        System.out.println("=================================");




        ArrayList<ProbKelas> alProbKelas = learn("");


        Connection conn=null;
        PreparedStatement pInsertProbKelas = null;
        PreparedStatement pInsertProbKata = null;

        clearTable("modelnb_prob_kelas");
        clearTable("modelnb_prob_kata");



        String strCon = "jdbc:mysql://"+dbName+"?user="+userName+"&password="+password;
        try  {
            //ambil id_class
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(strCon);
            conn.setAutoCommit(false);


            //tulis ke database


            //String strSQLinsertProbKelas =   "insert into modelnb_prob_kelas " +
             //       "(id_kelas,nama_class,prob_class,jumkata_class,prob_freq_nol) values (?,?,?,?,?)";
            String strSQLinsertProbKelas =   "insert into modelnb_prob_kelas " +
                   "(id_kelas,prob_class,jumkata_class,prob_freq_nol) values (?,?,?,?)";

            String strSQLinsertProbKata  =   "insert into modelnb_prob_kata  " +
                    "(id_kelas,kata,prob) values (?,?,?)";

            pInsertProbKelas  =  conn.prepareStatement (strSQLinsertProbKelas);
            pInsertProbKata   =  conn.prepareStatement (strSQLinsertProbKata);

            for (ProbKelas pk: alProbKelas) {
                pInsertProbKelas.setInt(1,pk.idKelas);
                //pInsertProbKelas.setString(2,pk.namaKelas);
                pInsertProbKelas.setDouble(2,pk.probKelas);  //geser
                pInsertProbKelas.setInt(3,pk.countWord.size());
                pInsertProbKelas.setDouble(4,pk.probDef);
                pInsertProbKelas.executeUpdate();
                for (Map.Entry<String,Double> entry : pk.probWord.entrySet()) {
                    pInsertProbKata.setInt(1,pk.idKelas);
                    String kata = entry.getKey();
                    Double probKata = entry.getValue();
                    pInsertProbKata.setString(2,kata);
                    pInsertProbKata.setDouble(3,probKata);
                    pInsertProbKata.addBatch();
                }
            }
            pInsertProbKata.executeBatch();

        }

        catch (Exception e)
        {
            //ROLLBACK
            logger.log(Level.SEVERE, null, e);
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    logger.log(Level.SEVERE, null, e1);
                }
                System.out.println("Fatal Error, rollback...");
                //isError = true;
            }
        }
        finally {
            try {
                pInsertProbKata.close();
                pInsertProbKelas.close();
                conn.commit();
                conn.setAutoCommit(true);
                conn.close();
            } catch (Exception e) {
                logger.log(Level.SEVERE, null, e);
                //isError = true;
            }
        }
    }


    public void xFoldCrossVal() {
	/*  hitung akurasi model
     *  sebelumnya sudah dipanggil learnToDB


	 *  tabel input harus ditambahkan field partisi (int, indexed)
	 *  field class sudah terdefinisi
	 *  tabel partisi tsb
	 *
	 *
	 *  menambahkan partisi
	 *  ALTER TABLE articles_prepro ADD COLUMN `partisi` INT NULL DEFAULT '0' AFTER `sanggahan_dukungan`;
	 *  ALTER TABLE articles_prepro ADD INDEX `partisi` (`partisi`);
	 */

        int jumFold = 10;

        //cek validitas model

        //partisi
        //hitung jumlah record total
        //bagi dengan jumFold
        //loop untuk semua data, secara random
        //isi field partisi dengan angka yg akan menjadi group (menggunakan mod)

        //bagian where-nya nanti diganti
        //String strSQLJumTw         = "select count(*)  from tw_jadi where " +fieldClass+" is not null";
        String strSQLJumTw  = String.format("select count(*) from %s where %s is not null",
                                        namaTableIn,namaFieldClass);
        //String strSQLtw            = "select id_internal from tw_jadi where " +fieldClass+" is not null order by rand()";
        String strSQLtw    = String.format("select %s from %s where %s is not null order by rand()"
                                      ,namaFieldIdIn, namaTableIn, namaFieldClass);
        //String strSQLupdatePartisi = "update tw_jadi set partisi = ? where id_internal= ?";
        String strSQLupdatePartisi= String.format("update %s set partisi = ? where %s= ?"
                                      ,namaTableIn,namaFieldIdIn);

        //String strSQLtwPartisi     = "select id_internal,text,text_prepro,"+fieldClass+" from tw_jadi where partisi = ? and " +fieldClass+" is not null";
        String strSQLtwPartisi     = String.format("select %s,%s,%s from %s where partisi = ? and %s is not null"
                                      ,namaFieldIdIn,namaFieldTeksIn,namaFieldClass,namaTableIn,namaFieldClass);

        Connection conn=null;
        PreparedStatement pJumTw = null;
        PreparedStatement pTw = null;
        PreparedStatement pUpdatePartisi = null;
        PreparedStatement pTwPartisi = null;
        ResultSet rsJumTw = null;
        ResultSet rsTw = null;
        ResultSet rsTwPartisi = null;


        try  {
            //ambil id_class
            Class.forName("com.mysql.jdbc.Driver");
            String strCon = "jdbc:mysql://"+dbName+"?user="+userName+"&password="+password;
            conn = DriverManager.getConnection(strCon);
            conn.setAutoCommit(false);
            pTwPartisi = conn.prepareStatement(strSQLtwPartisi);
            pJumTw  =  conn.prepareStatement (strSQLJumTw);
            rsJumTw = pJumTw.executeQuery();
            int jumTw;
            if (rsJumTw.next()) {
                jumTw = rsJumTw.getInt(1);
                System.out.println("Jumlah tweet"+jumTw);
            } else {
                System.out.println("tidak ada ada atau error mengakses db, abort");
                System.exit(1);
            }

            //ambil data tweet
            pTw = conn.prepareStatement(strSQLtw);
            pUpdatePartisi = conn.prepareStatement(strSQLupdatePartisi);
            rsTw = pTw.executeQuery();
            int cc = 0;
            while (rsTw.next()) {
                int id = rsTw.getInt(1);
                int partisi = (int) (cc % jumFold+1);
//       		 System.out.println("id="+id);
//       		 System.out.println("partisi:"+partisi);
                pUpdatePartisi.setInt(1, partisi);
                pUpdatePartisi.setInt(2, id);
                pUpdatePartisi.addBatch();
                cc++;
            }
            pUpdatePartisi.executeBatch();

            ArrayList<ProbKelas> model;
            double totAkurasi=0;
            //loop untuk setiap partisi
            for (int i=1;i<=jumFold;i++)  {  //partisi mulai dari 1
                //learn, lalu klasifikasikan
                System.out.println("Proses partisi ke:"+i);
                System.out.println("learn");
                model = learn("and partisi<>"+i);   //partisi i menjadi testing, partisi lainnya menjadi training  jd tidak masuk
//       		 for (ProbKelas pk: model) {
//       			 pk.print();
//       		 }


                //loop untuk semua docs dalam partisi i
                pTwPartisi.setInt(1,i);
                rsTwPartisi = pTwPartisi.executeQuery();
                ProbKelas pk;
                int jumCocok = 0;
                int jumSalah = 0;
                while (rsTwPartisi.next()) {
                    int id = rsTwPartisi.getInt(1);
                    //String twOrig = rsTwPartisi.getString(2); //belum diprepro
                    String tw = rsTwPartisi.getString(2);  //geser dari 3 jadi 2
                    int kelas = rsTwPartisi.getInt(3);
                    pk = classifyMem(model,tw);
//       			 System.out.println("Kelas yang benar:"+kelas);
//       			 System.out.println("Kelas prediksi:"  +pk.idKelas);
                    if (pk.idKelas == kelas) {
                        // System.out.println("cocok:");
                        jumCocok++;
                    } else {
                        // System.out.println("tdk cocok:");

                        System.out.println("teks:"+tw);
                        System.out.println("id:"+id);
                        System.out.println("Kelas yang benar:"+kelas);
                        System.out.println("Kelas prediksi:"  +pk.idKelas);
                        System.out.println();
                        jumSalah++;
                    }
                }
                System.out.println("Jum cocok:"+jumCocok);
                System.out.println("Jum salah:"+jumSalah);
                double akurasi = (double) jumCocok / (jumCocok+jumSalah);
                totAkurasi = totAkurasi + akurasi;
                System.out.println( "Akurasi:"+akurasi);
                //System.exit(1); //debug dulu
            }
            double avgAkurasi = totAkurasi / jumFold;
            System.out.println();
            System.out.println("rata2 akurasi="+avgAkurasi);

        }  catch (Exception e)
        {
            //ROLLBACK
            logger.log(Level.SEVERE, null, e);
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    logger.log(Level.SEVERE, null, e1);
                }
                System.out.println("Fatal Error, rollback...");
                //isError = true;
            }
        }
        finally {
            try {
                pJumTw.close();
                pTw.close();
                pUpdatePartisi.close();
                rsTw.close();
                rsJumTw.close();
                conn.commit();
                conn.setAutoCommit(true);
                conn.close();
            } catch (Exception e) {
                logger.log(Level.SEVERE, null, e);
                //isError = true;
            }
        }
    }

    public static void main(String[] args) {
        NaiveBayes t = new NaiveBayes();
        t.dbName="localhost/news";
        t.userName="news";
        t.password="news";
        t.namaTableIn="articles_prepro";
        t.namaFieldIdIn="id";
        t.namaFieldTeksIn="teks_prepro";
        t.namaFieldClass ="kelas";

        //t.learnToDB();

        t.xFoldCrossVal();
    }



}
