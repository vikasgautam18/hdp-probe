package com.gautam.mantra.spark.extras;

import com.gautam.mantra.commons.Product;
import com.gautam.mantra.commons.Sales;
import com.gautam.mantra.commons.Seller;
import org.apache.spark.sql.*;

import java.util.Map;

import static org.apache.spark.sql.functions.desc;

public class SalesAnalysis {

    private final String APP_NAME = "shop.data.analysis1.application";
    public final String SALES_IN_PATH = "sales.dataset.hdfs.path";
    public final String SELLER_IN_PATH = "seller.dataset.hdfs.path";
    public final String PRODUCT_IN_PATH = "product.dataset.hdfs.path";
    private Map<String, String> properties;

    SparkSession spark = SparkSession.builder()
            .appName(properties.get(APP_NAME)).getOrCreate();

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
        Dataset<Sales> sales = readSalesDataset(properties.get(SALES_IN_PATH));
        System.out.printf("The count of sales dataset is :: %s%n", sales.count());

        Dataset<Seller> sellers = readSellerDataset(properties.get(SELLER_IN_PATH));
        System.out.printf("The count of sellers dataset is :: %s%n", sellers.count());

        Dataset<Product> products = readProductDataset(properties.get(PRODUCT_IN_PATH));
        System.out.printf("The count of product dataset is :: %s%n", products.count());

        System.out.println("The number of products which have been sold atleast once:: "
                + getProductsSoldAtleastOnce(sales));

        Row row = getMostPopularProduct(sales);
        System.out.printf("The product with Id '%s' is the most popular one with over %s items sold%n",
                row.getAs("product_id"), row.getAs("count_sold"));

        spark.close();
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
