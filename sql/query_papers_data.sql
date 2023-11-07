    WITH
    papers as (
        SELECT "Rank" rank
            , "Publication ID" publication_id
            , "DOI" doi
            , "PMID" pmid
            , "PMCID" pmcid
            , "Title" title
            , "Abstract" abstract
            , "Acknowledgements" acknowledgements
            , "Source title" source_title
            , "Publisher" publisher
            , "MeSH terms" mesh_terms
            , "Publication Date" publication_date
            , "PubYear" pub_year
            , "Volume" volume
            , "Issue" issue
            , "Open Access" open_access
            , "Publication Type" publication_type
            , "Authors" authors
            , "Authors Affiliations" authors_affiliations
            , "Research Organizations - standardized" research_organizations
            , "City of Research organization" city_of_research
            , "State of Research organization" state_of_research
            , "Country of Research organization" country_of_research
            , "Funder" funder
            , "UIDs of supporting grants" uids_supporting_grants
            , "Times cited" times_cited
            , "Recent citations" recent_citations
            , "Dimensions URL" dimensions_url
            , "Source Linkout" source_linkout
            , "Fields of Research (ANZSRC 2020)" fields_of_research
            , "RCDC Categories" rcdc_categories
            , string_to_array("Authors", '; ') authors_array
        FROM papersdb
    )
    , AUTHORS AS (
        SELECT publication_id
             , unnest(authors_array) authors
        FROM papers
    )
    , COUNTER_PUBLICATIONS AS (
        SELECT authors
             , count(1) num_publications
        FROM AUTHORS
        GROUP BY 1
    )
    , PUBLICATION_COUNTER AS (
        SELECT A.publication_id, A.authors, B.num_publications
        FROM AUTHORS A
        LEFT JOIN COUNTER_PUBLICATIONS B ON A.authors = B.authors
    )
    , STATS_CITATIONS AS (
        SELECT publication_id
             , MAX(num_publications) max_publications_per_author
             , round(AVG(num_publications), 2) avg_publications_per_author
             , SUM(num_publications) total_publications_authors
             , COUNT(num_publications) num_authors
        FROM PUBLICATION_COUNTER
        GROUP BY 1
    )
    , DATA_PAPERS AS (
        SELECT A.publication_id
             , A.rank
             , A.title
             , case when length(A.abstract) < 3000 then A.abstract else substr(A.abstract, 1, 3000) end abstract
             , A.publisher
             , A.mesh_terms
             , A.publication_date
             , A.open_access
             , A.authors
             , B.avg_publications_per_author
             , B.total_publications_authors
             , B.max_publications_per_author
             , B.num_authors
             , A.research_organizations
             , A.dimensions_url
        FROM PAPERS A
        LEFT JOIN STATS_CITATIONS B ON A.publication_id=B.publication_id
    )
    SELECT *
    FROM DATA_PAPERS
    WHERE PUBLICATION_ID IN CHOSEN_IDS