package com.dto;

import java.io.Serializable;

public  class Click implements Serializable {
    private int userId;
    private int itemId;
    private int clicktime;
    private long timestamp;

    public Click() {}

    public Click(int userId, int itemId, int clicktime, long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.clicktime = clicktime;
        this.timestamp = timestamp;
    }

    public Click(int userId, int itemId, int clicktime) {
        this.userId = userId;
        this.itemId = itemId;
        this.clicktime = clicktime;
    }


    public int getUserId() {
        return userId;
    }

    public int getItemId() {
        return itemId;
    }

    public float getClicktime() {
        return clicktime;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public static Click parseRating(String str) {
        String[] fields = str.split("::");
        if (fields.length != 4) {
            throw new IllegalArgumentException("Each line must contain 4 fields");
        }
        int userId = Integer.parseInt(fields[0]);
        int itemId = Integer.parseInt(fields[1]);
        int clicktime = Integer.parseInt(fields[2]);
        long timestamp = Long.parseLong(fields[3]);
        return new Click(userId, itemId, clicktime);
    }
}
