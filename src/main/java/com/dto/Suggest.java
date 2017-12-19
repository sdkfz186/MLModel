package com.dto;

import java.sql.Date;

public class Suggest {
    private int userId;

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public Date getCreatetime() {
        return createtime;
    }

    public void setCreatetime(Date createtime) {
        this.createtime = createtime;
    }

    public int getItemId() {
        return itemId;
    }

    public void setItemId(int itemId) {
        this.itemId = itemId;
    }

    private int itemId;
    private Date createtime;

    @Override
    public String toString() {
        return "Suggest{" +
                "userId=" + userId +
                ", itemId=" + itemId +
                ", createtime=" + createtime +
                '}';
    }
}
