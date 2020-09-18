package com.gautam.mantra.commons;

public class ContactRecord {

    private String rowkey;
    private String officeAddress;
    private String officePhone;
    private String personalName;
    private String personalPhone;

    public ContactRecord(String rowkey, String officeAddress, String officePhone, String personalName, String personalPhone) {
        this.rowkey = rowkey;
        this.officeAddress = officeAddress;
        this.officePhone = officePhone;
        this.personalName = personalName;
        this.personalPhone = personalPhone;
    }

    public String getRowKey() {
        return rowkey;
    }

    public void setRowKey(String rowKey) {
        this.rowkey = rowKey;
    }

    public String getOfficeAddress() {
        return officeAddress;
    }

    public void setOfficeAddress(String officeAddress) {
        this.officeAddress = officeAddress;
    }

    public String getOfficePhone() {
        return officePhone;
    }

    public void setOfficePhone(String officePhone) {
        this.officePhone = officePhone;
    }

    public String getPersonalPhone() {
        return personalPhone;
    }

    public void setPersonalPhone(String personalPhone) {
        this.personalPhone = personalPhone;
    }

    public String getPersonalName() {
        return personalName;
    }

    public void setPersonalName(String personalName) {
        this.personalName = personalName;
    }

    @Override
    public String toString() {
        return "ContactRecord{" +
                "rowkey='" + rowkey + '\'' +
                ", officeAddress='" + officeAddress + '\'' +
                ", officePhone='" + officePhone + '\'' +
                ", personalName='" + personalName + '\'' +
                ", personalPhone='" + personalPhone + '\'' +
                '}';
    }
}
