package com.example;

public class Question {

    private String category;
    private String air_date;
    private String question;
    private String value;
    private String answer;
    private String round;
    private String short_number;

    public Question(String category, String air_date, String question, String value, String answer, String round, String short_number) {
        this.category = category;
        this.air_date = air_date;
        this.question = question;
        this.value = value;
        this.answer = answer;
        this.round = round;
        this.short_number = short_number;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getAir_date() {
        return air_date;
    }

    public void setAir_date(String air_date) {
        this.air_date = air_date;
    }

    public String getQuestion() {
        return question;
    }

    public void setQuestion(String question) {
        this.question = question;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getAnswer() {
        return answer;
    }

    public void setAnswer(String answer) {
        this.answer = answer;
    }

    public String getRound() {
        return round;
    }

    public void setRound(String round) {
        this.round = round;
    }

    public String getShort_number() {
        return short_number;
    }

    public void setShort_number(String short_number) {
        this.short_number = short_number;
    }
}
