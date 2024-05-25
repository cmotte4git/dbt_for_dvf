import json 
import csv
import duckdb

#some analytics query down there
import duckdb
#duckdb.sql("SELECT * FROM  'data/64.csv.gz'")


duckdb.sql("CREATE VIEW v1 AS SELECT * FROM read_parquet('C:/Users/cmotte/DBT_FOR_DVF/output/dvf_full.parquet')")

duckdb.sql("""SELECT 
id_parcelle,
date_mutation,
valeur_fonciere,
last_valeur,
valeur_fonciere - last_valeur
FROM 
	(
	SELECT 
	id_parcelle,
	date_mutation,
	valeur_fonciere,
	lag(valeur_fonciere) over (partition by id_parcelle, lot1_numero, lot2_numero order by date_mutation asc) AS last_valeur
	FROM 
			(
			SELECT id_parcelle, date_mutation,lot1_numero, lot2_numero,
			sum(valeur_fonciere) valeur_fonciere
			FROM v1
			WHERE type_local IN ('Maison','Appartement','Dépendance')
			GROUP BY ALL
			)
	ORDER BY id_parcelle
	)
WHERE last_valeur <> valeur_fonciere
ORDER BY (valeur_fonciere - last_valeur) desc""").write_parquet("highest_profit.parquet")
