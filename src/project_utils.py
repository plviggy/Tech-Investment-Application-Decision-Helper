import string

import numpy as np
import pandas as pd
from tqdm import tqdm


def get_kw_data_in_df(df, searched_keywords):
    '''
    Looks for the existence of keywords in a text
    '''
    chars_to_delete = string.punctuation + string.digits + '\n≥'

    found_kw_data = []
    id_col = list(df)[0]

    n = len(df)
    for i, row in tqdm(df.iterrows(), total=n, position=0, leave=True):
        try:
            id_row = row[id_col]
            total_text = str(row['abstract']) + ' ' + str(row['title'])
            total_text = total_text.lower()
            total_text = total_text.replace('\n', ' ')

            clean_text = re.sub(f'[{chars_to_delete}]',  ' ', total_text)
            clean_text = re.sub(' +', ' ', clean_text)

            kw_found = [kw for kw in searched_keywords if kw in clean_text]

            if len(kw_found)>0:
                kw_data = {
                    id_col: id_row,
                    'keywords': kw_found
                }
                found_kw_data.append(kw_data)
        except:
            pass
    return pd.DataFrame(found_kw_data)