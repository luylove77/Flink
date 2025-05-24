package com.luy.bean;

public class ProductsDbz {

    // 表名
    String table;

    // 库名
    String db;

    // 字段
    Integer id;

    String name;

    String description;

    Integer age;

    // 操作类型
    String op;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    @Override
    public String toString() {
        return "ProductsDbz{" +
                "table='" + table + '\'' +
                ", db='" + db + '\'' +
                ", id=" + id +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", age=" + age +
                ", op='" + op + '\'' +
                '}';
    }


}
