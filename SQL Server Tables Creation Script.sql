USE MichaelN_Retail;
GO

/* ------
   1. RetailCategory
   === */

CREATE Table dbo.RetailCategory
(
    CategoryID INT IDENTITY(1,1) NOT NULL,
    CategoryName VARCHAR(100) NOT NULL,

    CONSTRAINT PK_RetailCategory
        PRIMARY KEY (CategoryID),

    CONSTRAINT UQ_RetailCategory_CategoryName
        UNIQUE (CategoryName)
);
GO


/* ***************************
   2. RetailCustomer
   =============**================= */

CREATE TABLE dbo.RetailCustomer
(
    CustomerID INT NOT NULL,
    Age INT NULL,
    Gender VARCHAR(20) NULL,
    Location VARCHAR(100) NULL,
    SubscriptionStatus VARCHAR(20) NULL,
    PreviousPurchases INT NULL,
    FrequencyOfPurchases VARCHAR(30) NULL,

    CONSTRAINT PK_RetailCustomer
        PRIMARY KEY (CustomerID)
);
GO


/* 
   3. RetailProduct
    */

CREATE TABLE dbo.RetailProduct
(
    ProductID INT IDENTITY(1,1) NOT NULL,
    ProductName VARCHAR(100) NOT NULL,
    CategoryID INT NOT NULL,
    Size VARCHAR(20) NULL,
    Color VARCHAR(50) NULL,

    CONSTRAINT PK_RetailProduct
        PRIMARY KEY (ProductID),

    CONSTRAINT FK_RetailProduct_Category
        FOREIGN KEY (CategoryID)
        REFERENCES dbo.RetailCategory(CategoryID)
);
GO


/* 
   4. RetailOrder
  */

CREATE TABLE dbo.RetailOrder
(
    OrderID INT IDENTITY(1,1) NOT NULL,
    CustomerID INT NOT NULL,
    OrderDate DATE NOT NULL,
    ShippingType VARCHAR(50) NULL,
    DiscountApplied VARCHAR(10) NULL,
    PromoCodeUsed VARCHAR(10) NULL,
    PaymentMethod VARCHAR(50) NULL,

    CONSTRAINT PK_RetailOrder
        PRIMARY KEY (OrderID),

    CONSTRAINT FK_RetailOrder_Customer
        FOREIGN KEY (CustomerID)
        REFERENCES dbo.RetailCustomer(CustomerID)
);
GO


/* ============================================================
   5. RetailOrderDetail
    */

CREATE TABLE dbo.RetailOrderDetail
(
    OrderDetailID INT IDENTITY(1,1) NOT NULL,
    OrderID INT NOT NULL,
    ProductID INT NOT NULL,
    PurchaseAmount DECIMAL(10,2) NOT NULL,
    Profit DECIMAL(10,2) NOT NULL,

    CONSTRAINT PK_RetailOrderDetail
        PRIMARY KEY (OrderDetailID),

    CONSTRAINT FK_RetailOrderDetail_Order
        FOREIGN KEY (OrderID)
        REFERENCES dbo.RetailOrder(OrderID),

    CONSTRAINT FK_RetailOrderDetail_Product
        FOREIGN KEY (ProductID)
        REFERENCES dbo.RetailProduct(ProductID)
);
GO


/* ------------------------------------
   6. RetailReview
   ============================================================ */

CREATE TABLE dbo.RetailReview
(
    ReviewID INT IDENTITY(1,1) NOT NULL,
    OrderDetailID INT NOT NULL,
    ReviewRating DECIMAL(3,1) NULL,

    CONSTRAINT PK_RetailReview
        PRIMARY KEY (ReviewID),

    CONSTRAINT FK_RetailReview_OrderDetail
        FOREIGN KEY (OrderDetailID)
        REFERENCES dbo.RetailOrderDetail(OrderDetailID)
);
GO


/* +++++++++++++++++
   Indexes
   */

CREATE INDEX IX_RetailOrder_CustomerID
ON dbo.RetailOrder(CustomerID);
GO

CREATE INDEX IX_RetailOrderDetail_OrderID
ON dbo.RetailOrderDetail(OrderID);
GO

CREATE INDEX IX_RetailOrderDetail_ProductID
ON dbo.RetailOrderDetail(ProductID);
GO

CREATE INDEX IX_RetailReview_OrderDetailID
ON dbo.RetailReview(OrderDetailID);
GO


/*EXEC sys.sp_cdc_enable_db;
Go

EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name = 'RetailCustomer',
    @role_name = NULL;

Go
*/

