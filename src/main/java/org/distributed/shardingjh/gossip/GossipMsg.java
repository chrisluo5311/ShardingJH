package org.distributed.shardingjh.gossip;

import com.google.gson.Gson;

public class GossipMsg {
    private String msgType;
    private String msgContent;
    private String senderId;
    private String receiverId;
    private String timestamp;
    private String signature;
    
    public enum Type {  HOSTDOWN, HOSTUP, HOSTREMOVE, HOSTADD, HOSTUPDATE,TABLEUPDATE}
    public String getMsgType() {
        return msgType;
    }

    public void setMsgType(String msgType) {
        this.msgType = msgType;
    }

    public String getMsgContent() {
        return msgContent;
    }

    public void setMsgContent(String msgContent) {
        this.msgContent = msgContent;
    }

    public String getSenderId() {
        return senderId;
    }

    public void setSenderId(String senderId) {
        this.senderId = senderId;
    }

    public String getReceiverId() {
        return receiverId;
    }

    public void setReceiverId(String receiverId) {
        this.receiverId = receiverId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }
    
    public String toJson() {
        return new Gson().toJson(this);
    }

    public static GossipMsg fromJson(String json) {
        return new Gson().fromJson(json, GossipMsg.class);
    }
}
