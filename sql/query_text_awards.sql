SELECT "RecordID" record_id,
       "Award Title" title,
       "abstract" abstract,
       "Award Year" award_year,
       replace("Award Amount",',','')::float award_amount
FROM sbir_award_data
