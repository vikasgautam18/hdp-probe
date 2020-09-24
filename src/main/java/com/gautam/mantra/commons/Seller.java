package com.gautam.mantra.commons;

public class Seller {

    private String seller_id;
    private String seller_name;
    private Long daily_target;

    public String getSeller_id() {
        return seller_id;
    }

    public void setSeller_id(String seller_id) {
        this.seller_id = seller_id;
    }

    public String getSeller_name() {
        return seller_name;
    }

    public void setSeller_name(String seller_name) {
        this.seller_name = seller_name;
    }

    public Long getDaily_target() {
        return daily_target;
    }

    public void setDaily_target(Long daily_target) {
        this.daily_target = daily_target;
    }

    @Override
    public String toString() {
        return "Seller{" +
                "seller_id='" + seller_id + '\'' +
                ", seller_name='" + seller_name + '\'' +
                ", daily_target=" + daily_target +
                '}';
    }
}
