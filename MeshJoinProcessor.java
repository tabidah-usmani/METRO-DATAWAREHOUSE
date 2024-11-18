package com.meshjoin;

import java.sql.PreparedStatement;
import java.sql.Date;
import java.util.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MeshJoinProcessor 
{
    private static final String DB_URL_TEMPLATE = "jdbc:mysql://localhost:3306/%s?useSSL=false";
    private static int PARTITION_SIZE = 100; // Size of each partition or batch

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        // Prompt user for database names and credentials
        System.out.print("Enter name of the datasource database: ");
        String datasourceName = scanner.nextLine();

        System.out.print("Enter name of the datawarehouse database: ");
        String datawarehouseName = scanner.nextLine();

        System.out.print("Enter database username: ");
        String user = scanner.nextLine();

        System.out.print("Enter database password: ");
        String pass = scanner.nextLine();

        // Construct database URLs
        String dbUrlSource = String.format(DB_URL_TEMPLATE, datasourceName);
        String dbUrlWarehouse = String.format(DB_URL_TEMPLATE, datawarehouseName);

        try (Connection connSource = DriverManager.getConnection(dbUrlSource, user, pass);
             Connection connWarehouse = DriverManager.getConnection(dbUrlWarehouse, user, pass)) 
        {
            meshJoin(connSource, connWarehouse);
        } 
        catch (SQLException e) 
        {
            e.printStackTrace();
        }
    }

    private static void meshJoin(Connection connSource, Connection connWarehouse) throws SQLException 
    {
        String transactionQuery = "SELECT * FROM transactions";
        try (Statement stmtTransactions = connSource.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
             ResultSet rsTransactions = stmtTransactions.executeQuery(transactionQuery)) 
        {

            // Stream transactions in segments
            while (rsTransactions.next()) 
            {
                Map<Integer, Transaction> transactionMap = readTransactionSegment(rsTransactions);
                Queue<Transaction> transactionQueue = new LinkedList<>(transactionMap.values());

                while (!transactionQueue.isEmpty()) 
                {
                    // Load partitions from MD tables
                    Map<Integer, Customer> customerPartition = loadCustomerPartition(connSource);
                    Map<Integer, Product> productPartition = loadProductPartition(connSource);

                    // Process transaction queue
                    processTransactions(transactionQueue, customerPartition, productPartition, connWarehouse, connSource);
                }
            }
        }
    }

    private static Map<Integer, Transaction> readTransactionSegment(ResultSet rsTransactions) throws SQLException 
    {
        Map<Integer, Transaction> transactionMap = new HashMap<>();
        int count = 0;

        do 
        {
            int orderId = rsTransactions.getInt("ORDER_ID");
            Date orderDate = rsTransactions.getDate("ORDER_DATE"); // java.sql.Date
            int productId = rsTransactions.getInt("PRODUCT_ID");
            int quantity = rsTransactions.getInt("QUANTITY");
            int customerId = rsTransactions.getInt("CUSTOMER_ID");

            transactionMap.put(orderId, new Transaction(orderId, orderDate, productId, quantity, customerId));
            count++;
        } 
        while (rsTransactions.next() && count < PARTITION_SIZE);

        return transactionMap;
    }

    private static Map<Integer, Customer> loadCustomerPartition(Connection connSource) throws SQLException 
    {
        Map<Integer, Customer> customerMap = new HashMap<>();
        String customerQuery = "SELECT * FROM customers LIMIT " + PARTITION_SIZE;
        try (PreparedStatement pstmt = connSource.prepareStatement(customerQuery);
             ResultSet rsCustomers = pstmt.executeQuery()) {
            while (rsCustomers.next()) 
            {
                int customerId = rsCustomers.getInt("CUSTOMER_ID");
                String customerName = rsCustomers.getString("CUSTOMER_NAME");
                String gender = rsCustomers.getString("GENDER");

                customerMap.put(customerId, new Customer(customerId, customerName, gender));
            }
        }
        return customerMap;
    }

    private static Map<Integer, Product> loadProductPartition(Connection connSource) throws SQLException 
    {
        Map<Integer, Product> productMap = new HashMap<>();
        String productQuery = "SELECT * FROM products LIMIT " + PARTITION_SIZE;
        try (PreparedStatement pstmt = connSource.prepareStatement(productQuery);
             ResultSet rsProducts = pstmt.executeQuery()) 
        {
            while (rsProducts.next()) 
            {
                int productId = rsProducts.getInt("PRODUCT_ID");
                String productName = rsProducts.getString("PRODUCT_NAME");
                double productPrice = rsProducts.getDouble("PRODUCT_PRICE");

                productMap.put(productId, new Product(productId, productName, productPrice));
            }
        }
        return productMap;
    }

    private static void processTransactions(Queue<Transaction> transactionQueue,
                                            Map<Integer, Customer> customerPartition,
                                            Map<Integer, Product> productPartition,
                                            Connection connWarehouse,
                                            Connection connSource) throws SQLException 
    {

        while (!transactionQueue.isEmpty()) 
        {
            Transaction transaction = transactionQueue.poll();
            Customer customer = customerPartition.get(transaction.getCustomerId());
            Product product = productPartition.get(transaction.getProductId());

            if (customer != null && product != null) 
            {
                // Load store and supplier details
                String productQuery = "SELECT STORE_ID, STORE_NAME, SUPPLIER_ID, SUPPLIER_NAME FROM products WHERE PRODUCT_ID = ?";
                try (PreparedStatement pstmtProduct = connSource.prepareStatement(productQuery)) 
                {
                    pstmtProduct.setInt(1, transaction.getProductId());

                    try (ResultSet rsProductDetails = pstmtProduct.executeQuery()) 
                    {
                        if (rsProductDetails.next()) 
                        {
                            int storeId = rsProductDetails.getInt("STORE_ID");
                            String storeName = rsProductDetails.getString("STORE_NAME");
                            int supplierId = rsProductDetails.getInt("SUPPLIER_ID");
                            String supplierName = rsProductDetails.getString("SUPPLIER_NAME");

                            // Ensure product, customer, store, and supplier exist in warehouse
                            ensureProductExists(connWarehouse, product);
                            ensureCustomerExists(connWarehouse, customer);
                            ensureStoreExists(connWarehouse, storeId, storeName);
                            ensureSupplierExists(connWarehouse, supplierId, supplierName);

                            // Insert enriched transaction into sales
                            double totalSales = product.getProductPrice() * transaction.getQuantity();
                            insertSales(connWarehouse, transaction, totalSales, storeId, supplierId);
                        }
                    }
                }
            }
        }
    }

    private static void ensureProductExists(Connection connWarehouse, Product product) throws SQLException 
    {
        String checkProductQuery = "SELECT PRODUCT_ID FROM product WHERE PRODUCT_ID = ?";
        try (PreparedStatement pstmtCheck = connWarehouse.prepareStatement(checkProductQuery)) 
        {
            pstmtCheck.setInt(1, product.getProductId());

            try (ResultSet rsCheck = pstmtCheck.executeQuery()) 
            {
                if (!rsCheck.next()) 
                {
                    String insertProductQuery = "INSERT INTO product (PRODUCT_ID, PRODUCT_NAME, PRODUCT_PRICE) VALUES (?, ?, ?)";
                    try (PreparedStatement pstmtInsert = connWarehouse.prepareStatement(insertProductQuery)) 
                    {
                        pstmtInsert.setInt(1, product.getProductId());
                        pstmtInsert.setString(2, product.getProductName());
                        pstmtInsert.setDouble(3, product.getProductPrice());
                        pstmtInsert.executeUpdate();
                    }
                }
            }
        }
    }

    private static void ensureCustomerExists(Connection connWarehouse, Customer customer) throws SQLException 
    {
        String checkCustomerQuery = "SELECT CUSTOMER_ID FROM customer WHERE CUSTOMER_ID = ?";
        try (PreparedStatement pstmtCheck = connWarehouse.prepareStatement(checkCustomerQuery)) 
        {
            pstmtCheck.setInt(1, customer.getCustomerId());

            try (ResultSet rsCheck = pstmtCheck.executeQuery()) 
            {
                if (!rsCheck.next()) 
                {
                    String insertCustomerQuery = "INSERT INTO customer (CUSTOMER_ID, CUSTOMER_NAME, GENDER) VALUES (?, ?, ?)";
                    try (PreparedStatement pstmtInsert = connWarehouse.prepareStatement(insertCustomerQuery)) 
                    {
                        pstmtInsert.setInt(1, customer.getCustomerId());
                        pstmtInsert.setString(2, customer.getCustomerName());
                        pstmtInsert.setString(3, customer.getGender());
                        pstmtInsert.executeUpdate();
                    }
                }
            }
        }
    }

    private static void ensureStoreExists(Connection connWarehouse, int storeId, String storeName) throws SQLException 
    {
        String checkStoreQuery = "SELECT STORE_ID FROM store WHERE STORE_ID = ?";
        try (PreparedStatement pstmtCheck = connWarehouse.prepareStatement(checkStoreQuery)) 
        {
            pstmtCheck.setInt(1, storeId);

            try (ResultSet rsCheck = pstmtCheck.executeQuery()) 
            {
                if (!rsCheck.next()) 
                {
                    String insertStoreQuery = "INSERT INTO store (STORE_ID, STORE_NAME) VALUES (?, ?)";
                    try (PreparedStatement pstmtInsert = connWarehouse.prepareStatement(insertStoreQuery)) 
                    {
                        pstmtInsert.setInt(1, storeId);
                        pstmtInsert.setString(2, storeName);
                        pstmtInsert.executeUpdate();
                    }
                }
            }
        }
    }

    private static void ensureSupplierExists(Connection connWarehouse, int supplierId, String supplierName) throws SQLException 
    {
        String checkSupplierQuery = "SELECT SUPPLIER_ID FROM supplier WHERE SUPPLIER_ID = ?";
        try (PreparedStatement pstmtCheck = connWarehouse.prepareStatement(checkSupplierQuery)) 
        {
            pstmtCheck.setInt(1, supplierId);

            try (ResultSet rsCheck = pstmtCheck.executeQuery()) 
            {
                if (!rsCheck.next()) 
                {
                    String insertSupplierQuery = "INSERT INTO supplier (SUPPLIER_ID, SUPPLIER_NAME) VALUES (?, ?)";
                    try (PreparedStatement pstmtInsert = connWarehouse.prepareStatement(insertSupplierQuery)) 
                    {
                        pstmtInsert.setInt(1, supplierId);
                        pstmtInsert.setString(2, supplierName);
                        pstmtInsert.executeUpdate();
                    }
                }
            }
        }
    }

    private static void insertSales(Connection connWarehouse, Transaction transaction, double totalSales, int storeId, int supplierId) throws SQLException 
    {
        String insertQuery = "INSERT INTO sales (ORDER_ID, ORDER_DATE, QUANTITY, CUSTOMER_ID, PRODUCT_ID, STORE_ID, SUPPLIER_ID, TOTAL_SALES) " +
                             "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = connWarehouse.prepareStatement(insertQuery)) 
        {
            pstmt.setInt(1, transaction.getOrderId());
            pstmt.setDate(2, transaction.getOrderDate());
            pstmt.setInt(3, transaction.getQuantity());
            pstmt.setInt(4, transaction.getCustomerId());
            pstmt.setInt(5, transaction.getProductId());
            pstmt.setInt(6, storeId);
            pstmt.setInt(7, supplierId);
            pstmt.setDouble(8, totalSales);
            pstmt.executeUpdate();
        }
    }
}

// Supporting classes
class Transaction 
{
    private final int orderId;
    private final Date orderDate; // java.sql.Date
    private final int productId;
    private final int quantity;
    private final int customerId;

    public Transaction(int orderId, Date orderDate, int productId, int quantity, int customerId) 
    {
        this.orderId = orderId;
        this.orderDate = orderDate;
        this.productId = productId;
        this.quantity = quantity;
        this.customerId = customerId;
    }

    public int getOrderId() 
    { 
    	return orderId; 
    }
    public Date getOrderDate()
    { 
    	return orderDate; 
    }
    public int getProductId() 
    { 
    	return productId; 
    }
    public int getQuantity() 
    {
    	return quantity; 
    }
    public int getCustomerId() 
    { 
    	return customerId; 
    }
}

class Customer 
{
    private final int customerId;
    private final String customerName;
    private final String gender;

    public Customer(int customerId, String customerName, String gender) 
    {
        this.customerId = customerId;
        this.customerName = customerName;
        this.gender = gender;
    }

    public int getCustomerId() 
    { 
    	return customerId; 
    }
    public String getCustomerName() 
    { 
    	return customerName; 
    }
    public String getGender() 
    { 
    	return gender; 
    }
}

class Product 
{
    private final int productId;
    private final String productName;
    private final double productPrice;

    public Product(int productId, String productName, double productPrice)
    {
        this.productId = productId;
        this.productName = productName;
        this.productPrice = productPrice;
    }

    public int getProductId() 
    { 
    	return productId;
    }
    public String getProductName() 
    { 
    	return productName; 
    }
    public double getProductPrice() 
    { 
    	return productPrice; 
    }
}
