import duckdb

#piece of code to import directly from files.data.gouv

con = duckdb.connect()
con.sql(""" CREATE VIEW dvf_2019_2023 AS 
            SELECT * FROM read_csv('https://files.data.gouv.fr/geo-dvf/latest/csv/2019/full.csv.gz' , null_padding = TRUE, ignore_errors=true, 
                            types={'lot1_numero': 'VARCHAR', 'lot5_surface_carrez': 'VARCHAR'})
        UNION
            SELECT * FROM read_csv('https://files.data.gouv.fr/geo-dvf/latest/csv/2020/full.csv.gz' , null_padding = TRUE, ignore_errors=true, 
                            types={'lot1_numero': 'VARCHAR', 'lot5_surface_carrez': 'VARCHAR'})
        UNION
            SELECT * FROM read_csv('https://files.data.gouv.fr/geo-dvf/latest/csv/2021/full.csv.gz' , null_padding = TRUE, ignore_errors=true, 
                            types={'lot1_numero': 'VARCHAR', 'lot5_surface_carrez': 'VARCHAR'})
        UNION
            SELECT * FROM read_csv('https://files.data.gouv.fr/geo-dvf/latest/csv/2022/full.csv.gz' , null_padding = TRUE, ignore_errors=true, 
                           types={'lot1_numero': 'VARCHAR', 'lot5_surface_carrez': 'VARCHAR'})
         UNION
            SELECT * FROM read_csv('https://files.data.gouv.fr/geo-dvf/latest/csv/2023/full.csv.gz' , null_padding = TRUE, ignore_errors=true, 
                            types={'lot1_numero': 'VARCHAR', 'lot5_surface_carrez': 'VARCHAR'})
        
        
        """)

con.sql("SHOW TABLES").show()
con.sql("SELECT * FROM dvf_2019_2023").write_parquet('data/dvf_full.parquet')
#con.sql("SELECT COUNT(*) FROM dvf_2019_2023").show()

#.write_parquet('output/64.parquet') 


#import locally 

#con = duckdb.connect()
#con.sql("CREATE VIEW v1 AS SELECT * FROM read_csv('C:/Users/cmotte/DBT_FOR_DVF/data/dvf*', null_padding = TRUE, ignore_errors=true, types={'lot1_numero': 'VARCHAR'})")
#con.sql("SELECT * FROM v1").write_parquet('output/dvf_full.parquet')
#con.sql("SHOW TABLES").show()



