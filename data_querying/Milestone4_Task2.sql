SELECT locality, COUNT (store_code) AS total_no_stores
FROM dim_store_details AS locality
WHERE locality IN ('Chapletown','Belper', 'Bushey', 'Exeter','High Wycombe', 'Arbroath', 'Rutherglen')
GROUP BY locality
ORDER BY total_no_stores DESC;