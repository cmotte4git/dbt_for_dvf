with dvf as (
SELECT 
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
			FROM {{ ref('full_load') }}
			WHERE type_local IN ('Maison','Appartement','Dépendance')
			GROUP BY ALL
			)
	ORDER BY id_parcelle
	)
WHERE last_valeur <> valeur_fonciere
ORDER BY (valeur_fonciere - last_valeur) desc
)

SELECT * FROM dvf 
