import streamlit as st

st.set_page_config(page_title="SERP Keyword Clustering App v1.1", page_icon="🔎",
                   layout="wide")  # needs to be the first thing after the streamlit import

from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import time
from io import BytesIO

import chardet
import pandas as pd
import requests
from stqdm import stqdm

st.write(
    "Made in [![this is an image link](https://i.imgur.com/iIOA6kU.png)](https://www.streamlit.io/) with :heart: by [@LeeFootSEO](https://twitter.com/LeeFootSEO)")
st.title("SERP Clustering Tool")

# streamlit variables
value_serp_key = st.sidebar.text_input('Enter your ValueSERP Key')
url_filter = st.sidebar.text_input('Enter your website domain for rank tracking')
threads = st.sidebar.slider("Set number of threads", value=15, min_value=1, max_value=25)
common_urls = st.sidebar.slider("Set number of common urls to match", value=3, min_value=2, max_value=5)
device_select = st.sidebar.radio("Select the device to search Google", ('Mobile', 'Desktop', 'Tablet'))
location_select = st.sidebar.selectbox('Select the location to search Google', (
    'United States', 'United Kingdom', 'Australia', 'India', 'Spain', 'Italy', 'Canada', 'Germany', 'Ireland', 'France',
    'Holland'))

uploaded_file = st.file_uploader("Upload your keyword report")

# store the data
query_l = []
link_l = []
position_l = []

if uploaded_file is not None:
    try:
        result = chardet.detect(uploaded_file.getvalue())
        encoding_value = result["encoding"]
        if encoding_value == "UTF-16":
            white_space = True
        else:
            white_space = False
        df = pd.read_csv(uploaded_file, encoding=encoding_value, delim_whitespace=white_space, on_bad_lines='skip')
        number_of_rows = len(df)
        if number_of_rows == 0:
            st.caption("Your sheet seems empty!")
        with st.expander("↕ View raw data", expanded=False):
            st.write(df)
    except UnicodeDecodeError:
        st.warning("""🚨 The file doesn't seem to load. Check the filetype, file format and Schema""")

else:
    st.info("👆 Upload a .csv or .txt file first.")
    st.stop()

st.subheader("Please Select the Keyword Column")
kw_col = st.selectbox('Select the keyword column:', df.
columns)
df.rename(columns={kw_col: "query"}, inplace=True)
df_virgin = df.copy()


def get_serp(q):
    params = {
        'api_key': value_serp_key,
        'q': q,
        'location': location_select,
        'include_fields': 'organic_results',
        'location_auto': True,
        'device': device_select,
        'output': 'json',
        'page': '1',
        'num': '10'
    }

    response = requests.get('https://api.valueserp.com/search', params)
    response_data = json.loads(response.text)
    result = response_data.get('organic_results')

    for var in result:
        try:
            link_l.append(var['link'])
        except Exception:
            link_l.append("")

        try:
            position_l.append(var['position'])
        except Exception:
            position_l.append("")

        try:
            query_l.append(q)
        except Exception:
            query_l.append("")


def extract_url(url):
    split_url = url.split("/")
    return split_url[2]


def fetch_keywords(keywords_l, threads):
    with ThreadPoolExecutor(max_workers=threads) as executor:
        results = [executor.submit(get_serp, keyword) for keyword in keywords_l]

        for f in stqdm(as_completed(results), total=len(results), desc="Keywords", leave=True,
                       bar_format='{l_bar}{bar}| elapsed: {elapsed} remaining: {remaining}'):
            pass


st.subheader("Start the SERP Clustering")

if st.button('Start Clustering'):
    fetch_keywords(list(df['query']), threads)
    df_serp = pd.DataFrame({'query': query_l, 'position': position_l, 'url': link_l})
    df_serp['domain'] = df_serp['url'].apply(lambda x: extract_url(x))
    df_merge = pd.merge(df, df_serp, on='query', how='left')
    df_merge = df_merge.drop(columns=['position', 'url'])
    df_pivot = df_merge.pivot_table(index='query', columns='domain', aggfunc='size', fill_value=0)

    if url_filter:
        df_pivot = df_pivot[df_pivot.index.str.contains(url_filter)]

    df_pivot['match'] = df_pivot.apply(lambda x: x.sum(), axis=1)

    df_pivot_sorted = df_pivot.sort_values(by=['match'], ascending=False)

    df_final = df_pivot_sorted[df_pivot_sorted['match'] >= common_urls]
    df_final.reset_index(inplace=True)
    df_final.rename(columns={'query': 'Keyword'}, inplace=True)

    st.subheader("Results")
    st.dataframe(df_final, width=1000)
