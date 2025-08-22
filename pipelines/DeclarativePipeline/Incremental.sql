-- Incremental sales_east
INSERT INTO sales_east VALUES
(6, 101, 203, 1, 100.00, '2025-08-02 09:00:00'),
(7, 106, 206, 2, 250.00, '2025-08-02 09:15:00');

-- Incremental sales_west
INSERT INTO sales_west VALUES
(13, 112, 212, 2, 300.00, '2025-08-02 09:30:00'),
(14, 107, 208, 1, 130.00, '2025-08-02 09:45:00');

-- Price change for product_id 203
INSERT INTO products VALUES
(203, 'Monitor', 'Electronics', 90.00, '2025-08-02 08:00:00');

-- Name change for product_id 208
INSERT INTO products VALUES
(208, 'Desk Lamp', 'Furniture', 130.00, '2025-08-02 08:10:00');

-- Region change for customer 103
INSERT INTO customers VALUES
(103, 'Charlie', 'Central', '2025-08-02 08:30:00');

-- Name correction for customer 107
INSERT INTO customers VALUES
(107, 'George Smith', 'West', '2025-08-02 08:40:00');