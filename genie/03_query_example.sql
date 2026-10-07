-- P3: Qual companhia apresenta a melhor pontualidade e a menor taxa de cancelamento?

SELECT
    nome_companhia,
    COUNT(*) AS voos,
    ROUND(
        100.0 * try_divide(
            SUM(CASE WHEN voo_cancelado = true THEN 1 ELSE 0 END),
            COUNT(*)
        ),
        2 
    ) AS pct_cancelados,
    ROUND(
        100.0 * try_divide(
            SUM(CASE WHEN partida_pontual = true THEN 1 ELSE 0 END),
            SUM(CASE WHEN partida_pontual IS NOT NULL THEN 1 ELSE 0 END)
        ),
        2
    ) AS pct_pontuais,
    ROUND(AVG(atraso_partida_min),2) AS atraso_medio_min
FROM voebem.gold.obt_voos
GROUP BY nome_companhia
HAVING COUNT(*) >= 10000
ORDER BY
    pct_pontuais DESC,
    pct_cancelados ASC;