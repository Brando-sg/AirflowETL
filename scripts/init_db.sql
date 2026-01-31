-- DIM Productos
CREATE TABLE IF NOT EXISTS dim_products (
    product_id VARCHAR(50) PRIMARY KEY, 
    category VARCHAR(50),               
    description VARCHAR(255),
    price NUMERIC(10, 2),
    cost NUMERIC(10, 2),
    supplier VARCHAR(100)
);

-- DIM Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT PRIMARY KEY,      
    full_date DATE,               
    year INT,                     
    month INT,                    
    month_name VARCHAR(20),
    month_short VARCHAR(3),       
    day INT,                      
    day_of_week INT,              
    day_name VARCHAR(20),         
    quarter INT,                  
    is_weekend BOOLEAN            
);

-- DIM Clientes
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(255)
);

-- DIM Sucursal
CREATE TABLE IF NOT EXISTS dim_sucursal (
    sucursal_id INT PRIMARY KEY,
    sucursal_name VARCHAR(50)
);

INSERT INTO dim_sucursal (sucursal_id, sucursal_name)
VALUES (1, 'Web'), (2, 'Catálogo')
ON CONFLICT (sucursal_id) DO NOTHING;

-- Fact Orders
CREATE TABLE IF NOT EXISTS fact_orders (
    id SERIAL PRIMARY KEY,
    transaction_id INT,
    customer_id INT,
    invoice_num NUMERIC(20, 0),
    date_id INT,                  
    product_pcode VARCHAR(50),    
    sucursal_id INT,              
    quantity INT,
    FOREIGN KEY (product_pcode) REFERENCES dim_products(product_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (sucursal_id) REFERENCES dim_sucursal(sucursal_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id)
);

-- Indices
CREATE INDEX idx_fact_date ON fact_orders(date_id);
CREATE INDEX idx_fact_product ON fact_orders(product_pcode);