SELECT 
    CAST(dp.kiva_partner_id AS VARCHAR) AS "Kode Mitra Lapangan", 
    COUNT(fl.loan_id)                   AS "Total Transaksi",
    SUM(fl.funded_amount)               AS "Total Dana Disalurkan (USD)",
    AVG(dr.mpi)                         AS "Rata-rata Skor Kemiskinan"
FROM iceberg.gold.fact_loans fl
JOIN iceberg.gold.dim_partner dp ON fl.partner_id = dp.partner_id
JOIN iceberg.gold.dim_region dr ON fl.region_id = dr.region_id
WHERE dr.mpi > 0.3 -- FILTER UTAMA: Membatasi hanya pada wilayah kemiskinan ekstrem
GROUP BY dp.kiva_partner_id
ORDER BY "Total Dana Disalurkan (USD)" DESC
LIMIT 20;
