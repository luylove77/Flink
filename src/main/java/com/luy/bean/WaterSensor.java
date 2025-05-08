package com.luy.bean;

public class WaterSensor {
    // 假设 WaterSensor 类有以下属性
    public String id;
    public long timestamp;
    public int vc;

    // 公共的无参构造函数
    public WaterSensor() {
    }

    /**
     * 带参构造方法，用于初始化 WaterSensor 对象
     *
     * @param id        传感器的 ID
     * @param timestamp 时间戳
     * @param vc        水位值
     */
    public WaterSensor(String id, long timestamp, int vc) {
        this.id = id;
        this.timestamp = timestamp;
        this.vc = vc;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getVc() {
        return vc;
    }

    public void setVc(int vc) {
        this.vc = vc;
    }

    @Override
    public String toString() {
        return "WaterSensor{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", vc=" + vc +
                '}';
    }
}
