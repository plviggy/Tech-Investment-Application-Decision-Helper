WITH
EVENT_DATASET AS (
    WITH
    TIME_EVENTS AS (
        SELECT p.patentid
             , arr.value->>'title' as event
             , arr.value->>'date' as date
             , arr.value
        FROM patentdb p, jsonb_array_elements(time_events) as arr
        WHERE jsonb_typeof(time_events) = 'array'
    )
    , RANKED_EVENTS AS (
        SELECT
            *
            , row_number() over (partition by patentid order by date asc) rown_asc
            , row_number() over (partition by patentid order by date desc) rown_desc
        FROM TIME_EVENTS
    )
    , AGG_REGISTRIES AS (
        SELECT patentid
    -- Events: publication, external-priority, reassignment, priority,
    -- filed, legal-status, litigation, granted
             , sum(CASE WHEN event = 'publication' then 1 else 0 end) num_publications
             , sum(CASE WHEN event = 'granted' then 1 else 0 end) num_grants
             , sum(CASE WHEN event = 'litigation' then 1 else 0 end) num_litigations
             , min(CASE WHEN event = 'publication' then date end) first_publication
             , max(CASE WHEN event = 'publication' then date end) last_publication
             , min(CASE WHEN event = 'granted' then date end) first_grant
             , max(CASE WHEN event = 'granted' then date end) last_grant
        FROM TIME_EVENTS
        GROUP BY 1
    )
    , FIRST_REGISTRY AS (
        SELECT *
        FROM RANKED_EVENTS
        WHERE rown_asc = 1
        --    AND  patentid='AU1769501A'
    )
    , LAST_REGISTRY AS (
        SELECT *
        FROM RANKED_EVENTS
        WHERE rown_desc = 1
        --    AND  patentid='AU1769501A'
    )
    SELECT ar.*
         , fr.event first_status
         , fr.date first_status_date
         , lr.event last_status
         , lr.date last_status_date
         --, round(abs(extract(days from ar.first_publication::timestamp - fr.date::timestamp))/30.4) months_from_first_activity_to_first_publication
    FROM AGG_REGISTRIES ar
    LEFT JOIN FIRST_REGISTRY fr ON ar.patentid=fr.patentid
    LEFT JOIN LAST_REGISTRY lr ON ar.patentid=lr.patentid
)
, INVENTOR_DATASET AS (
    WITH
    INVENTORS AS (
        SELECT patentid
             , unnest(inventor_name) inventor
        FROM patentdb
    )
    , COUNTER_PATENTS AS (
        SELECT inventor
             , count(1) num_patents
        FROM INVENTORS
        GROUP BY 1
    )
    , PATENT_COUNTER AS (
        SELECT A.patentid, A.inventor, B.num_patents
        FROM INVENTORS A
        LEFT JOIN COUNTER_PATENTS B ON A.inventor = B.inventor
    )
    , STATS_INVENTIONS AS (
        SELECT patentid
             , MAX(num_patents) max_patents_per_inventor
             , round(AVG(num_patents), 2) avg_patents_per_inventor
             , SUM(num_patents) total_patents_inventors
             , COUNT(num_patents) num_inventors
        FROM PATENT_COUNTER
        GROUP BY 1
    )
    SELECT *
    FROM STATS_INVENTIONS
)
, DATA_PATENTS AS (
    SELECT A.patentid
         , A.inventor_name
         , A.assignee_name_origin
         , A.assignee_name_current
         , A.countrycode
         , A.title
         , CASE WHEN length(A.abstract) < 3000 THEN A.abstract ELSE substr(A.abstract, 1, 3000) END abstract

         , B.num_publications
         , B.num_grants
         , B.num_litigations
         , B.first_publication
         , B.last_publication
         , B.first_grant
         , B.last_grant
         , B.first_status
         , B.first_status_date
         , B.last_status
         , B.last_status_date

         , C.max_patents_per_inventor
         , C.avg_patents_per_inventor
         , C.total_patents_inventors
         , C.num_inventors
    FROM patentdb A
    LEFT JOIN  EVENT_DATASET B ON A.patentid = B.patentid
    LEFT JOIN INVENTOR_DATASET C ON A.patentid = C.patentid
)
SELECT *
FROM DATA_PATENTS
WHERE patentid IN CHOSEN_IDS
--    LIMIT 300