package com.gautam.mantra.commons;

import java.util.Date;

public class Sales {

    private String order_id;
    private String product_id;
    private String seller_id;
    private Integer num_pieces_sold;
    private String bill_raw_text;
    private Date order_date;

    public String getOrder_id() {
        return order_id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }

    public String getProduct_id() {
        return product_id;
    }

    public void setProduct_id(String product_id) {
        this.product_id = product_id;
    }

    public String getSeller_id() {
        return seller_id;
    }

    public void setSeller_id(String seller_id) {
        this.seller_id = seller_id;
    }

    public Integer getNum_pieces_sold() {
        return num_pieces_sold;
    }

    public void setNum_pieces_sold(Integer num_pieces_sold) {
        this.num_pieces_sold = num_pieces_sold;
    }

    public String getBill_raw_text() {
        return bill_raw_text;
    }

    public void setBill_raw_text(String bill_raw_text) {
        this.bill_raw_text = bill_raw_text;
    }

    public Date getOrder_date() {
        return order_date;
    }

    public void setOrder_date(Date order_date) {
        this.order_date = order_date;
    }

    @Override
    public String toString() {
        return "Sales{" +
                "order_id='" + order_id + '\'' +
                ", product_id='" + product_id + '\'' +
                ", seller_id='" + seller_id + '\'' +
                ", num_pieces_sold=" + num_pieces_sold +
                ", bill_raw_text='" + bill_raw_text + '\'' +
                ", order_date=" + order_date +
                '}';
    }
}
