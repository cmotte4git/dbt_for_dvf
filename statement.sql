--Select statement for all dvf

-- 2019
SELECT date_mutation FROM read_csv('data\dvf_2019.csv.gz' , null_padding = TRUE, ignore_errors=true, types={'lot1_numero': 'VARCHAR'});

-- 2020
SELECT date_mutation FROM read_csv('data\dvf_2020.csv.gz' , null_padding = TRUE, ignore_errors=true, types={'lot1_numero': 'VARCHAR'});

-- 2021
SELECT date_mutation FROM read_csv('data\dvf_2021.csv.gz' , null_padding = TRUE, ignore_errors=true, types={'lot1_numero': 'VARCHAR'});

-- 2022
SELECT date_mutation FROM read_csv('data\dvf_2022.csv.gz' , null_padding = TRUE, ignore_errors=true, types={'lot1_numero': 'VARCHAR'});

-- 2023
SELECT date_mutation FROM read_csv('data\dvf_2023.csv.gz' , null_padding = TRUE, ignore_errors=true, types={'lot1_numero': 'VARCHAR'});

---Select all dvf
SELECT date_mutation FROM read_csv('DVF_data\data\dvf*' , null_padding = TRUE, ignore_errors=true, types={'lot1_numero': 'VARCHAR'});

--Create view
CREATE VIEW v1 AS SELECT * FROM read_csv('data\dvf*'  , null_padding = TRUE, ignore_errors=true, types={'lot1_numero': 'VARCHAR'});

--SELECT count(*) FROM read_csv('data\dvf*'  , null_padding = TRUE, ignore_errors=true, types={'lot1_numero': 'VARCHAR'});
--(SELECT * FROM v1).write_parquet('output/dvf_full.parquet');

COPY (SELECT * FROM v1) TO 'output\dvf_full.parquet' (COMPRESSION ZSTD);

SELECT COUNT(*) FROM 'output\dvf_full.parquet';

CREATE VIEW v1 AS SELECT * FROM read_csv('https://files.data.gouv.fr/geo-dvf/latest/csv/2023/departements/01.csv.gz' , null_padding = TRUE, ignore_errors=true)

https://files.data.gouv.fr/geo-dvf/latest/csv/2021/full.csv.gz