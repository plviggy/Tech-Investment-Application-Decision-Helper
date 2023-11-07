    WITH AWARDS AS (
        SELECT "RecordID" record_id
               , "Company" company
               , "Award Title" award_title
               , "Agency" agency
               , "Branch" branch
               , "Phase" phase
               , TO_DATE("Proposal Award Date", 'MM/DD/YYYY')::timestamp proposal_date
               , TO_DATE("Contract End Date", 'MM/DD/YYYY')::timestamp contract_end_date
               , "Award Year" award_year
               , replace("Award Amount",',','')::float award_amount
               , "Number Employees" num_employees
               , "City" city
               , case when length("abstract") < 3000 then "abstract" else substr("abstract", 1, 3000) end abstract
               , "Contact Name" contact_name
               , "Contact Phone" contact_phone
               , "Contact Email" contact_email
        FROM sbir_award_data
    )
    , COMPANY_DATA AS (
        SELECT company
             , count(1) num_awards_company
             , max(award_amount) max_award_amount_company
             , sum(award_amount) total_award_amount_company
             , round(avg(award_amount)) avg_award_amount_company
        FROM AWARDS
        GROUP BY 1
    )
    , DATA_AWARDS AS (
        SELECT A.record_id
             , A.award_title
             , A.abstract
             , A.agency
             , A.phase
             , A.num_employees
             , A.company
             , B.num_awards_company
             , A.award_amount
             , B.max_award_amount_company
             , B.total_award_amount_company
             , B.avg_award_amount_company
             , A.proposal_date::date proposal_date
             , A.contract_end_date::date contract_end_date
             , abs(extract(days from A.contract_end_date - A.proposal_date)) award_days_duration
             , A.contact_name
             , A.contact_phone
             , A.contact_email
        FROM AWARDS A
        LEFT JOIN COMPANY_DATA B ON A.company = B.company
    )
    SELECT *
    FROM DATA_AWARDS
    WHERE record_id IN CHOSEN_IDS
--    LIMIT 300