CREATE TABLE etl.etl_logs (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    process_name VARCHAR(100),
    layer VARCHAR(20), -- Bronze / Silver / Gold
    status VARCHAR(20), -- START / SUCCESS / FAILED
    rows_processed INT NULL,
    error_message VARCHAR(MAX) NULL,
    log_time DATETIME DEFAULT GETDATE()
);
