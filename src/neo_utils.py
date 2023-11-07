import decimal
import json
import os
import re
import requests
import string
import sys
import warnings

from bs4 import BeautifulSoup
from dotenv import load_dotenv, find_dotenv
import matplotlib.pyplot as plt
import nltk
import numpy as np
from neo4j import GraphDatabase
import pandas as pd
import psycopg2
from pymongo import MongoClient
from rake_nltk import Rake
import seaborn as sns
from tqdm import tqdm


#---(1) Find list of technologies with expertise ------------------------------
def neo4j_find_technologies_w_exp(graphdb):
    
    #Initialize empty tech list
    tech_list =[]
    
    #Initialize session in neo4j and run query
    #Query: finds technologies and the corresponding number of companies with expertise. Ranked in desc order.
    session = graphdb.session()
    q1 = "match p= (a:technology)-[r:hasExpertiseIn]-(b) return a.name AS Technology, count(p) AS Degree order by Degree desc"
    nodes = session.run(q1)
    
    #Populate tech list 
    for node in nodes:
        tech = node.value('Technology').lower()
        tech_list.append(tech)
    
    #Close session and return tech list
    session.close()
    
    return tech_list


#---(2) Search ACM Ontology for keywords---------------------------------------
def neo4j_get_ontology_keywords(graphdb,technology):
    
    #Initialize keyword list and set initial keyword to be the name of the technology itself. 
    kw_list =[technology.lower()]
    
    #Initialize session in neo4j and run query
    #Query: retrieve up to 6 levels of the keyword ontology when looking up a particular technology. Case insensitive search using neo4j regexp
    session = graphdb.session()
    q2="MATCH p=(j:skos__Concept)-[r:skos__narrower*..6]->(b) WHERE j.skos__prefLabel =~ '(?i)" + technology + "' RETURN b.skos__prefLabel AS keyword"
    nodes = session.run(q2)
    
    #Populate keyword list (all lower case)
    for node in nodes:
        keyword = node.value('keyword').lower()
        kw_list.append(keyword)
        
    #Close session and return tech list
    session.close()

    return kw_list


#############################################################
################# NEO4J - UPLOADING DATA ####################
#############################################################

def upload_nodes_df_to_neo4j(label:str, data:pd.DataFrame, session, features=[]):
    '''
    Uploads the elements of a DataFrame as nodes. 
    It will upload all the content as attributes if not specified.
    
    Parameters
    ----------
        label (str): Name of the label node
        data (pd.DataFrame): Data with the nodes to be uploaded
        session (neo4j connector): Connector or cursor from the neo4j library
        features (list): Columns to upload (all if not specified)
    '''
    
    df = data[features].copy() if features != [] else data.copy()
    
    querys = []
    ln = len(df)
    for i, row in tqdm(df.iterrows(), total=ln, position=0, leave=True):   
#    for i, row in df.iterrows():
        drow=row.to_dict()
        query = f'create ({label[0].lower()}:{label} {{'

        n = len(drow)
        # Creating the query
        for i, key in enumerate(drow):
            val = drow[key]
            if isinstance(val, str):
                qry = f"{key}:'{val}'"
            elif isinstance(val, int) or isinstance(val, float):
                qry = f"{key}:{val}"
            else:
                qry = f"{key}:'{val}'"
            query += f'{qry}, ' if i < n-1 else qry 
        query += '})'
        #print(query)
        session.run(query)
        querys.append(query)   

        
def upload_edges_df_to_neo4j(label_a, label_b, label_rel, data, session):
    '''
    Uploads the relationship betweern elements as edges. 
    
    Parameters
    ----------
        label_a (str): Label of the origin node
        label_b (str): Lbel of the end node
        label_rel (str): Label of the relationship
        data (pd.DataFrame): Data with the relationships to be uploaded
        session (neo4j connector): Connector or cursor from the neo4j library
    '''
    
    col_a, col_b = list(data)
    la = label_a[0].lower()
    lb = label_b[0].lower()
    lr = label_rel[0].lower()
    df = data.copy()
    
    querys = []
    ln = len(df)
    for i, row in tqdm(df.iterrows(), total=ln, position=0, leave=True):   
#    for i, row in df.iterrows():
    
        drow=row.to_dict()

        n = len(drow)
        keys = list(drow)

        query = f'''
        match ({la}:{label_a}), ({lb}:{label_b}) 
        where {la}.{col_a} = '{drow[col_a]}' AND {lb}.{col_b} = '{drow[col_b]}'
        create ({la})-[{lr}:{label_rel}]->({lb})
        '''
        #print(query)
        session.run(query)

    querys.append(query)         
        

def get_df_relationship(df):
    '''
    Creates a dataframe unnesting a column of type str[]
    
    Parameters
    ----------
        df (pd.DataFrame): DataFrame with relationships
    
    Returns
    -------
        df_rels (pd.DataFrame): DataFrame with the relationships unnested
    '''
    
    rels = []
    id_col, agg_col = list(df)
    for i, row in df.iterrows():
        for elm in row[agg_col]:
            id_val = row[id_col]
            rels.append([id_val, elm])
    df_rels = pd.DataFrame(rels, columns=[id_col, agg_col])
    return df_rels        