drop database if exists ramalah;
create database ramalah;
use ramalah;

create table customer_dim (
    customer_id int primary key,
    gender varchar(10),
    age int,
    occupation varchar(50),
    city_category varchar(10),
    stay_in_current_city_years varchar(10),
    marital_status varchar(10)
);

create table product_dim (
    product_id varchar(20) primary key,
    product_category varchar(100),
    price decimal(10,2),
    store_id int,
    store_name varchar(150),
    supplier_id int,
    supplier_name varchar(150)
);

create table time_dim (
    date_id int primary key auto_increment,
    full_date date,
    day_of_week varchar(15),
    month varchar(15),
    quarter int,
    season varchar(50),
    year int
);

create table saleFact (
    sales_id int primary key auto_increment,
    order_id int,
    customer_id int,
    product_id varchar(20),
    date_id int,
    quantity int,
    purchase_amount decimal(12,2),
	constraint FK_sale_customer foreign key (customer_id) references customer_dim(customer_id),
    constraint FK_sale_product foreign key (product_id) references product_dim(product_id),
    constraint FK_sale_time foreign key (date_id) references time_dim(date_id)
);