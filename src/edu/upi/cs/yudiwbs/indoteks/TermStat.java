package edu.upi.cs.yudiwbs.indoteks;

/**
 * Digunakan untuk menghitung nilai rata-rata term, biasanya dikombinasikan dengan hashmap
 * (penggunaan lihat TFIDF dan KMeans)
 *
 * @author Yudi Wibisono (yudi@upi.edu)
 */



public class TermStat  {
    public String term;
    private  int freq;
    private double totVal; // total value
    private double avg;


    public TermStat(String term,double totVal) {
        freq=1;
        avg = 0;
        this.term = term;
        this.totVal = totVal;
    }

    public void incFreq() {
        freq++;
    }

    public  void addVal(double val) {
        totVal += val;
        incFreq();
    }

    public void calcAvg() {
        avg = totVal / freq;
    }

    /**
     * panggil dulu calcAvg!
     * @return
     */
    public double getAvg() {
        return avg;
    }

    public int getFreq() {
        return freq;
    }

    public double getTotVal() {
        return totVal;
    }
} // class termStat

