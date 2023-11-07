select "Publication ID" publication_id, 
   "Title" title,
   "Abstract" abstract,
   array_length(string_to_array("Authors", ';'), 1)  num_authors,
   "PubYear" pub_year
from papersdb