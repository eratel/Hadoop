package com.abhsy.json;


//{"movie":"1193","rate":"5","timeStamp":"978300760","uid":"1"}
public class MovieRate {
    private String movie;
    private String rate;
    private String timeStamp;
    private String uid;

    public String getMovie() {
        return movie;
    }

    public void setMovie(String movie) {
        this.movie = movie;
    }

    public String getRate() {
        return rate;
    }

    public void setRate(String rate) {
        this.rate = rate;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String toString() {

        return movie + "\t" + rate + "\t" + timeStamp + "\t" + uid;

    }
}
