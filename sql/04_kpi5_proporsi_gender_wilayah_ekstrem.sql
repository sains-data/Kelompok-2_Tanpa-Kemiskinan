SELECT 
    dr.country,
    fl.funded_amount,
    CASE 
        WHEN fl.borrower_genders = 'female' OR fl.borrower_genders LIKE 'female%' AND fl.borrower_genders NOT LIKE '%male%' THEN 'Perempuan'
        WHEN fl.borrower_genders = 'male' OR fl.borrower_genders LIKE 'male%' AND fl.borrower_genders NOT LIKE '%female%' THEN 'Laki-laki'
        ELSE 'Campuran'
    END AS kategori_gender -- TRANSFORMATION LOGIC: Pembersihan data gender mentah
FROM iceberg.gold.fact_loans fl
JOIN iceberg.gold.dim_region dr ON fl.region_id = dr.region_id
WHERE dr.mpi > 0.3; -- FILTER UTAMA: Fokus sasaran SDG 1 (Tanpa Kemiskinan)
