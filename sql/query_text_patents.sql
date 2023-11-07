SELECT patentid,
   title,
   abstract, 
   coalesce(jsonb_array_length(cite -> 'backwardReferences'), 0) num_backward,
   coalesce(jsonb_array_length(cite -> 'forwardReferencesOrig'), 0) num_forward     
FROM patentdb
