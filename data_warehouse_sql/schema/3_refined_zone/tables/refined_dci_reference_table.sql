CREATE TABLE IF NOT EXISTS ft_ds_refined.dci_reference_data (
    PRIMARY KEY (zip_code),
    zip_code VARCHAR(15),
    chapter_name VARCHAR(100),
    chapter_id CHAR(18),
    dci_score INTEGER
);