SELECT 
    ds.sector,
    SUM(fl.funded_amount) as total_pendanaan,
    COUNT(fl.loan_id) as jumlah_transaksi
FROM iceberg.gold.fact_loans fl
JOIN iceberg.gold.dim_sector ds ON fl.sector_id = ds.sector_id
JOIN iceberg.gold.dim_region dr ON fl.region_id = dr.region_id
WHERE dr.mpi > 0.3
GROUP BY ds.sector
ORDER BY total_pendanaan DESC
