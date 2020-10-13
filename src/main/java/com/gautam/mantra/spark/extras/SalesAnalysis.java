package com.gautam.mantra.spark.extras;

import com.gautam.mantra.commons.Product;
import com.gautam.mantra.commons.Sales;
import com.gautam.mantra.commons.Seller;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;

public class SalesAnalysis {

    private final Map<String, String> properties;

    private SparkSession spark;

    public SalesAnalysis (Map<String, String> properties){
        this.properties = properties;
    }

    public long getProductsSoldAtleastOnce(Dataset<Sales> sales) {
        return sales.select("product_id").distinct().count();
    }

    public Row getMostPopularProduct(Dataset<Sales> sales) {
        return sales.groupBy(sales.col("product_id"))
                .agg(functions.count(sales.col("product_id")).as("count_sold"))
                .orderBy(desc("count_sold"))
                .takeAsList(1).get(0);
    }

    public void process(){
        String APP_NAME = properties.get("shop.data.analysis1.application");
        spark = SparkSession.builder()
                .appName(APP_NAME).getOrCreate();

        Dataset<Sales> sales = readSalesDataset(properties.get("sales.dataset.hdfs.path"));
        System.out.printf("The count of sales dataset is :: %s%n", sales.count());

        Dataset<Seller> sellers = readSellerDataset(properties.get("seller.dataset.hdfs.path"));
        System.out.printf("The count of sellers dataset is :: %s%n", sellers.count());

        Dataset<Product> products = readProductDataset(properties.get("product.dataset.hdfs.path"));
        System.out.printf("The count of product dataset is :: %s%n", products.count());

        System.out.println("The number of products which have been sold atleast once:: "
                + getProductsSoldAtleastOnce(sales));

        Row row = getMostPopularProduct(sales);
        System.out.printf("The product with Id '%s' is the most popular one with over %s items sold%n",
                row.getAs("product_id"), row.getAs("count_sold"));

        Dataset<Row> distinctProductsPerDay = getDistinctProductsSoldPerDay(sales);
        distinctProductsPerDay.show();

        getAverageRevenueOfOrders(sales, products);

        spark.close();
    }

    private void getAverageRevenueOfOrders(Dataset<Sales> sales, Dataset<Product> products) {
        Dataset<Row> joined = sales.join(products,
                sales.col("product_id").equalTo(products.col("product_id")));

        System.out.println("The revenue of orders ::");
        joined.withColumn("revenue",
                functions.col("num_pieces_sold").$times(col("price")))
                .select("order_id", "revenue")
                .orderBy(col("revenue").desc())
        .show(100);
    }

    public Dataset<Row> getDistinctProductsSoldPerDay(Dataset<Sales> sales) {
        return sales.withColumn("date_modified", functions.col("date").cast(DataTypes.DateType))
                .groupBy("date_modified").agg(functions.countDistinct("product_id"));
    }

    public Dataset<Product> readProductDataset(String path) {
        return spark.read().parquet(path).as(Encoders.bean(Product.class));
    }

    public Dataset<Seller> readSellerDataset(String path) {
        return spark.read().parquet(path).as(Encoders.bean(Seller.class));
    }

    public Dataset<Sales> readSalesDataset(String path) {
        return spark.read().parquet(path).as(Encoders.bean(Sales.class));
    }
}
