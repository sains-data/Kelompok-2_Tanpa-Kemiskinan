SELECT 
    fl.loan_id,
    fl.term_in_months,
    CASE 
        WHEN dr.mpi >= 0.3 THEN '1. Ekstrem (MPI >= 0.3)'
        WHEN dr.mpi >= 0.1 AND dr.mpi < 0.3 THEN '2. Rentan (0.1 - 0.29)'
        ELSE '3. Aman (MPI < 0.1)'
    END AS kategori_kemiskinan -- REKAYASA FITUR: Kategori komparatif untuk Sumbu X
FROM iceberg.gold.fact_loans fl
JOIN iceberg.gold.dim_region dr ON fl.region_id = dr.region_id;
