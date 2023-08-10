'''
GDELT TO CSV NEWS DOWNLOADER:
This script downloads the news articles from the GDELT database and saves
them in a csv file.
'''
import os
import json
import requests
import re
import pandas as pd
from newspaper import Article, Config
from tqdm import tqdm
import joblib
import csv
import nltk
nltk.download('all')

# CARPETAS DE DATOS:
###############################################################################
if not os.path.exists('../data'):
    os.makedirs('../data')

# Comprobamos que existe la carpeta data/urls:
if not os.path.exists('../data/urls'):
    os.makedirs('../data/urls')

# Comprobamos que existe la carpeta data/articles:
if not os.path.exists('../data/articles'):
    os.makedirs('../data/articles')

if not os.path.exists('../data/processed'):
    os.makedirs('../data/processed')

# GDELT QUERYING:
###############################################################################

def api_url_constructor(query,
                        mode='artlist',
                        format='json',
                        records=250,
                        start_date=20200101000000,
                        end_date=20220131235959):
    query = query.replace(' ', '%20')
    url_head = "https://api.gdeltproject.org/api/v2/doc/doc?"
    body = f"query={query}&mode={mode}&format={format}&maxrecords={records}"
    params = f"&startdatetime={start_date}&enddatetime={end_date}"
    trans = "&trans=googtrans"
    
    full_url = url_head + body + params + trans
    
    return full_url


def query_cleaner(query):
    ''' 
    La API de GDELT no acepta caracteres especiales en la query.
    Los sustituimos por espacios.
    '''
    patrones = r'[^\w\s\"\(\)]+'
    query = re.sub(patrones, ' ', query)
    
    # Terminos de denominacion empresarial que no son utiles:
    terminos_eliminar = (
        r' Inc\.| Corp\.| Ltd\.| PLC| Co\.| S\.A\.|& KGaA|& KgaA|'
        r'Co\., Ltd\.|P L C|\(publ\)|\.Com|\.com|A/S|AB|AE|AG|AS|'
        r'ASA|Abp|CORP|Co|Corp|GmbH & KGaA|Inc|Inc\.|KGaA|LLC|LP|'
        r'LPG|LTD|Ltd|MFG|NL|NV|Oyj|PLC|Plc|RL|SA|SE|SGPS|SpA|plc|rp'
        )
    
    query = re.sub(terminos_eliminar,
                    '',
                    query,
                    flags=re.IGNORECASE)
    
    # Quitamos dobles espacios:
    query = re.sub(r'\s+', ' ', query)
    
    return query.strip()


def name_amplifier(name):
    '''
    A veces los nombres tienen demasiadas palabras.
    Esto hace la query muy específica y puede obviar resultados.
    Añadimos permutaciones de las palabras del nombre con 'OR'
    '''
    nombre_query = name # Por defecto, el nombre original
    
    if len(name.split(' '))>2:
        nombre_corto = ''
        nombre_corto = ' '.join(name.split()[:3])
        nombre_corto_2 = ' '.join(name.split()[:2])
        nombre_corto_3 = ' '.join(name.split()[:1])
        # Unimos con 'OR' los diferentes nombres:
        nombre_query = (
            f'("{nombre_corto_3}" OR "{name}" OR '
            f'"{nombre_corto}" OR "{nombre_corto_2}")'
        )
    
    return nombre_query


def query_completion(query, positives, negatives):
    '''
    Función que recibe una lista de positivos(tuplas) y otra de negativos.
    Los positivos se añaden a la query como:
    'AND ("positivo1" OR "positivo1_alt" OR ...)'
    'AND ("positivo2" OR "positivo2_alt" OR ...)'
    Los negativos se añaden como 'AND -negativo1 AND -negativo2 ...'
    '''

    # Si hay positivos, los añadimos a la query:
    positives_query = ''

    if positives:
        for p in positives:
            # Si solo hay un elemento en la tupla, no hace falta añadir OR:
            positive_to_add = f'"{p[0]}"'
           
            # Si hay más de un elemento, añadimos OR:
            if len(p)>1:
                for i in p[1:]:
                    positive_to_add += f' OR "{i}"'

            # El formato es AND ("positivo1" OR "positivo1_alt" OR ...)
            positives_query += f' AND ({positive_to_add})'

    negatives_query = ''
    if negatives:
        negative_to_add = f''
        for n in negatives:
                negative_to_add += f' AND -"{n}"'
        negatives_query += negative_to_add

    query += positives_query# + negatives_query
    
    return query


def name_to_query(name, positives, negatives):
    '''
    Función que recibe un nombre y devuelve una query de búsqueda.
    '''
    # Limpiamos el nombre:
    name = query_cleaner(name)
    # Añadimos permutaciones de palabras, 
    # pero si alguna palabra tiene 4 letras o menos no:
    palabras = name.split(' ')
    if len(palabras)>1:
        for p in palabras:
            if len(p)<=4:
                None
            else:
                name = name_amplifier(name)
    # Añadimos positivos y negativos:
    query = query_completion(name, positives, negatives)
    return query


def get_json(url, query, download = False):
    '''
    Función que recibe una url y devuelve un json o lo descarga
    '''
    try:
        results = requests.get(url=url).json()
    except:
        try:
            results = json.loads(
                requests.get(url=url).content.replace(b'\\', b'')
                )
        except:
            try:
                results = json.loads(urllib.request.urlopen(url)
                                     .read()
                                     .replace(b'\\', b'')
                                     .decode('utf-8', 'ignore'))
            except:
                # Dejamos la línea aunque haya errores:
                results = np.nan
    return results


def json_df(json_object):
    # Creamos un dataframe vacío:
    df_holder = pd.DataFrame()

    # Iteramos sobre las noticias:
    for noticia in json_object['articles']:
        # Datos de la noticia:
        url = noticia['url']
        title = noticia['title']
        date = noticia['seendate']
        domain = noticia['domain']
        language = noticia['language']
        country = noticia['sourcecountry']
        
        # Añadimos los datos al dataframe:
        nueva_fila = {'url': url,
                        'title': title,
                        'date': date,
                        'domain': domain,
                        'language': language,
                        'country': country}
        # Añadimos la nueva fila al dataframe:
        df_holder = pd.concat([df_holder,
                            pd.DataFrame.from_dict(
                                nueva_fila, 
                                orient='index').T],
                            ignore_index=True)
    return df_holder


# NEWS EXTRACTOR:
###############################################################################
# Creamos una función que extraiga el texto de las noticias válidas:
def extraer_texto(url):
    config = Config()
    config.browser_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' \
        'AppleWebKit/537.36 (KHTML, like Gecko) ' \
        'Chrome/88.0.4324.150 Safari/537.36'

    config.memoize_articles = False
    config.fetch_images = False
    try:
        a = Article(url)
        a.download()
        a.parse()
        a.nlp()
        autores = a.authors
        cuerpo = a.text
        keywords = a.keywords
        summary = a.summary
        fecha = a.publish_date
        return (autores, cuerpo, keywords, summary, fecha)
    except:
        try:
            a = Article(url)
            a.download()
            a.parse()
            a.nlp()
            autores = a.authors
            cuerpo = a.text
            keywords = a.keywords
            summary = a.summary
            fecha = a.publish_date
            return (autores, cuerpo, keywords, summary, fecha)
        except:
            return None


# PIPELINE:
###############################################################################        
# V_Final:
def recopilar_noticias(url):

    # Intentamos extraer los datos:
    try:
        autores, cuerpo, keywords, summary, fecha = extraer_texto(url)
    except:
        # Si no podemos, guardamos los datos previos y pasamos
        # a la siguiente iteración:
        autores = ''
        cuerpo = ''
        keywords = ''
        summary = ''
        fecha = ''
    
    # Si podemos, guardamos los datos previos y los nuevos:
    if autores is None:
        autores = ''
    if keywords is None:
        keywords = ''
    if fecha is not None:
        try:
            fecha = fecha.strftime('%Y-%m-%d')
        except:
            None
    else:
        fecha = ''
        # El cuerpo de la noticia puede contener saltos de línea,
        # lo que nos daría problemas al guardar el CSV:
    cuerpo = cuerpo.replace('\n', ' ')
    summary = summary.replace('\n', ' ')
    # A veces el cuerpo de la noticia contiene el caracter ';',
    # lo que nos daría problemas al guardar el CSV:
    cuerpo = cuerpo.replace(';', ',')
    summary = summary.replace(';', ',')
    # A veces hay otros caracteres que dan problemas, los quitamos:
    cuerpo = re.sub(r'[^\w\s\"\(\)]+', ' ', cuerpo)
    summary = re.sub(r'[^\w\s\"\(\)]+', ' ', summary)
    cuerpo = cuerpo.replace('  ', '')
    summary = summary.replace('  ', '')
    
    # Diccionario con los datos:
    nueva_fila = {'url': url,
                  'autores': autores,
                  'keywords': keywords,
                  'summary': summary,
                  'fecha': fecha,
                  'cuerpo': cuerpo}
    return nueva_fila

def pipeline_total(query, positives, negatives, start_date, end_date):
    final_query = query_completion(
        name_amplifier(query_cleaner(query)),
        positives,
        negatives)
    url = api_url_constructor(f'{final_query}')
    json_noticias = get_json(url, query, download = False)
    df_json = json_df(json_noticias)
    filas = joblib.Parallel(
        n_jobs=-1)(
            joblib.delayed(
                recopilar_noticias)(urls) for urls in tqdm(df_json['url']))
    for i in range(len(filas)):
        df_json.at[i,'autores'] = ";".join(filas[i]['autores'])
        df_json.at[i,'keywords'] = ";".join(filas[i]['keywords'])
        df_json.at[i,'summary'] = filas[i]['summary']
        df_json.at[i,'fecha'] = filas[i]['fecha']
        df_json.at[i,'cuerpo'] = filas[i]['cuerpo']
    return df_json