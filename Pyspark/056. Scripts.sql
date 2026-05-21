DROP TABLE IF EXISTS tbl_orders;


CREATE TABLE tbl_orders(
order_id varchar(10),
customer varchar(50),
product varchar(50),
quantity int,
price double,
order_date date
);


select * from  tbl_orders;

truncate table tbl_orders ;

DROP TABLE IF EXISTS tbl_sales;

CREATE TABLE tbl_sales(
SalesDate date,
Country varchar(30),
Product varchar(100),
SalesAmount float
);


select * from  tbl_sales;

truncate table tbl_sales ;

DROP TABLE IF EXISTS tbl_purchase;

CREATE TABLE tbl_purchase(
PurchaseDate date,
Country varchar(30),
Product varchar(100),
PurchaseAmount float
);


select * from  tbl_purchase;

truncate table tbl_purchase ;