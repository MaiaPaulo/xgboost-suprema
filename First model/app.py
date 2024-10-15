import streamlit as st
import pandas as pd
import joblib
import pickle
import cachetools
from snowflake.snowpark.context import get_active_session

session = get_active_session()


# Funcoes para ler arquivos do stage
@cachetools.cached(cache={})
def read_model():
    with session.file.get_stream('@"STREAMLIT"."DEV"."STREAMLIT_STAGE"/staged/best_model_no_players.pkl') as file:
        m = joblib.load(file)
        return m


def read_encoder():
    with session.file.get_stream('@"STREAMLIT"."DEV"."STREAMLIT_STAGE"/staged/new_encoder.pkl') as file:
        m = joblib.load(file)
        return m


def read_csv():
    with session.file.get_stream(
            '@"STREAMLIT"."DEV"."STREAMLIT_STAGE"/staged/feature_importances_no_players.csv') as file:
        m = pd.read_csv(file)
        return m


model = read_model()
feat_importances = read_csv()
encoder = read_encoder()
feat_importances.columns = ['Feature Importance', 'Value']

# Título do App
st.title(':spades: :blue[Modelo de previsão MTT] :spades:')

# Valores numericos de input para o modelo
buyin_price = st.number_input('Valor do Buy-in', min_value=0.0, format="%.2f")
rebuy_price = st.number_input('Valor do Rebuy', min_value=0.0, format="%.2f")
addon_price = st.number_input('Valor do Add-on', min_value=0.0, format="%.2f")
gtd_match = st.number_input('Valor Garantido', min_value=0.0, format="%.2f")

# Dados categoricos a serem transformados
match_type_detail_name = st.selectbox(
    'Tipo de Jogo',
    ('MTT-NLH', 'MTT-PLO4', 'MTT-PLO5', 'MTT-PLO6')
)

class_match = st.selectbox(
    'Classificação da Partida',
    ('Micro', 'Low', 'Medium', 'High')
)

day_of_week = st.selectbox(
    'Dia da Semana',
    ('sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday')
)

period = st.selectbox(
    'Período do Dia',
    ('morning', 'afternoon', 'evening', 'dawn')
)

# Dicionario com todos os dados brutos do usuario
user_input = {
    'BUYIN_PRICE': [buyin_price],
    'REBUY_PRICE': [rebuy_price],
    'ADDON_PRICE': [addon_price],
    'GTD_MATCH': [gtd_match],
    'MATCH_TYPE_DETAIL_NAME': [match_type_detail_name],
    'CLASS_MATCH': [class_match],
    'DAY_OF_WEEK': [day_of_week],
    'PERIOD': [period]
}

df_user_input = pd.DataFrame(user_input)

# Separacao de colunas categoricas e numericas
categorical_columns = ['MATCH_TYPE_DETAIL_NAME', 'CLASS_MATCH', 'DAY_OF_WEEK', 'PERIOD']
numerical_columns = ['BUYIN_PRICE', 'REBUY_PRICE', 'ADDON_PRICE', 'GTD_MATCH']

# Aplicacao do OnohotEncoder nos dados categoricos
df_categorical = df_user_input[categorical_columns]
encoded_input = encoder.transform(df_categorical)
encoded_input_df = pd.DataFrame(encoded_input, columns=encoder.get_feature_names_out(categorical_columns))

# Concat dos dois tipos de dados novamente
df_numerical = df_user_input[numerical_columns].reset_index(drop=True)
final_input_df = pd.concat([df_numerical, encoded_input_df], axis=1)

# Garantindo as colunas esperadas pelo modelo em ordem, caso nao tenha valor o 0 e adicionado
expected_columns = model.get_booster().feature_names
for col in expected_columns:
    if col not in final_input_df.columns:
        final_input_df[col] = 0
final_input_df = final_input_df[expected_columns]

# Botao para aplicaco do modelo
if st.button('Fazer Previsão e extrair dados iguais'):
    predicao = model.predict(final_input_df)
    if predicao[0] == 1:
        st.write('PREVISÃO DE LUCRO')
    else:
        st.write('PREVISÃO DE PREJUIZO')

    # Consulta SQL construída dinamicamente
    query = f"""
    SELECT DISTINCT(bp.id_match),
        bp.match_type_detail_name,
        bp.class_match,
        bp.total_players,
        ds.value_buyin AS buyin_price,
        ds.value_rebuy AS rebuy_price,
        ds.value_addon AS addon_price,
        bp.date_start_time_match_utc_local,
        CASE 
            WHEN EXTRACT(DOW FROM bp.date_start_time_match_utc_local) = 0 THEN 'sunday'
            WHEN EXTRACT(DOW FROM bp.date_start_time_match_utc_local) = 1 THEN 'monday'
            WHEN EXTRACT(DOW FROM bp.date_start_time_match_utc_local) = 2 THEN 'tuesday'
            WHEN EXTRACT(DOW FROM bp.date_start_time_match_utc_local) = 3 THEN 'wednesday'
            WHEN EXTRACT(DOW FROM bp.date_start_time_match_utc_local) = 4 THEN 'thursday'
            WHEN EXTRACT(DOW FROM bp.date_start_time_match_utc_local) = 5 THEN 'friday'
            WHEN EXTRACT(DOW FROM bp.date_start_time_match_utc_local) = 6 THEN 'saturday'
        END AS day_of_week,
        CASE
            WHEN EXTRACT(HOUR FROM bp.date_start_time_match_utc_local) BETWEEN 6 AND 11 THEN 'morning'
            WHEN EXTRACT(HOUR FROM bp.date_start_time_match_utc_local) BETWEEN 12 AND 17 THEN 'afternoon'
            WHEN EXTRACT(HOUR FROM bp.date_start_time_match_utc_local) BETWEEN 18 AND 23 THEN 'evening'
            ELSE 'dawn'
        END AS period,
        CAST(bp.gtd_match AS INTEGER) AS gtd_match,
        ROUND(bp.buyin_value, 2) AS buyin_earned,
        ROUND(bp.rebuy_value, 2) AS rebuy_earned,
        ROUND(bp.addon_value, 2) AS addon_earned,
        bp.overlay_match,
        CASE
            WHEN bp.gtd_match != 0 THEN ABS((bp.overlay_match / bp.gtd_match) * 100)
            ELSE 0
        END AS overlay_perc,
        CASE
            WHEN bp.overlay_match < 0 THEN 0
            WHEN bp.overlay_match = 0 THEN 1
            ELSE bp.overlay_match
        END AS overlay_match_bi,
        bp.duration_match
    FROM 
        poker_prod.poker.bi_product bp 
    JOIN
        (SELECT DISTINCT id_match, value_buyin, value_rebuy, value_addon
        FROM poker_prod.poker.dim_union_summary) ds
        ON bp.id_match = ds.id_match
    WHERE 
        bp.match_type_detail_name LIKE '{user_input['MATCH_TYPE_DETAIL_NAME'][0]}%'
        AND ds.value_buyin = {user_input['BUYIN_PRICE'][0]}
        AND ds.value_rebuy = {user_input['REBUY_PRICE'][0]}
        AND ds.value_addon = {user_input['ADDON_PRICE'][0]}
        AND bp.gtd_match = {user_input['GTD_MATCH'][0]}
        AND bp.class_match = '{user_input['CLASS_MATCH'][0]}'
        AND EXTRACT(DOW FROM bp.date_start_time_match_utc_local) = CASE 
            WHEN '{user_input['DAY_OF_WEEK'][0]}' = 'sunday' THEN 0
            WHEN '{user_input['DAY_OF_WEEK'][0]}' = 'monday' THEN 1
            WHEN '{user_input['DAY_OF_WEEK'][0]}' = 'tuesday' THEN 2
            WHEN '{user_input['DAY_OF_WEEK'][0]}' = 'wednesday' THEN 3
            WHEN '{user_input['DAY_OF_WEEK'][0]}' = 'thursday' THEN 4
            WHEN '{user_input['DAY_OF_WEEK'][0]}' = 'friday' THEN 5
            WHEN '{user_input['DAY_OF_WEEK'][0]}' = 'saturday' THEN 6
        END
        AND CASE
            WHEN '{user_input['PERIOD'][0]}' = 'morning' THEN EXTRACT(HOUR FROM bp.date_start_time_match_utc_local) BETWEEN 6 AND 11
            WHEN '{user_input['PERIOD'][0]}' = 'afternoon' THEN EXTRACT(HOUR FROM bp.date_start_time_match_utc_local) BETWEEN 12 AND 17
            WHEN '{user_input['PERIOD'][0]}' = 'evening' THEN EXTRACT(HOUR FROM bp.date_start_time_match_utc_local) BETWEEN 18 AND 23
            ELSE EXTRACT(HOUR FROM bp.date_start_time_match_utc_local) < 6
        END
        AND bp.duration_match > '00:05:00'
    """

    # Executar a consulta no Snowflake
    equal_data = session.sql(query).collect()

    st.subheader('Dados iguais no banco:')

    # Converter o resultado em um DataFrame e exibir o mesmo
    df_equal = pd.DataFrame(equal_data)

    if df_equal.empty == True:
        st.write('Sem dados similares')
    else:
        st.write(df_equal)
st.subheader("", divider='blue')

############################################### EXPANDER 1 ###############################################################################

# Buscar dados similares dentro do banco
# Criacao de input para dados numericos e reutilizacao para dados categoricos

expander_similar = st.expander("Busca de dados similares")
expander_similar.write("Os dados categóricos serão os mesmos do input inicial")
buyin_price_siml = expander_similar.slider("Selecione um range para o Buy-in", value=[0, 500])
rebuy_price_siml = expander_similar.slider("Selecione um range para o Re-buy", value=[0, 500])
addon_price_siml = expander_similar.slider("Selecione um range para o Add-on", value=[0, 500])
gtdmin_match_siml = expander_similar.number_input("Valor garantido min:", value=1)
gtdmax_match_siml = expander_similar.number_input("Valor garantido max:", value=100)

# Query dinamica aplicando range
query_sim = f"""
SELECT DISTINCT(bp.id_match),
    bp.match_type_detail_name,
    bp.class_match,
    bp.total_players,
    ds.value_buyin AS buyin_price,
    ds.value_rebuy AS rebuy_price,
    ds.value_addon AS addon_price,
    bp.date_start_time_match_utc_local,
    CASE 
        WHEN EXTRACT(DOW FROM bp.date_start_time_match_utc_local) = 0 THEN 'sunday'
        WHEN EXTRACT(DOW FROM bp.date_start_time_match_utc_local) = 1 THEN 'monday'
        WHEN EXTRACT(DOW FROM bp.date_start_time_match_utc_local) = 2 THEN 'tuesday'
        WHEN EXTRACT(DOW FROM bp.date_start_time_match_utc_local) = 3 THEN 'wednesday'
        WHEN EXTRACT(DOW FROM bp.date_start_time_match_utc_local) = 4 THEN 'thursday'
        WHEN EXTRACT(DOW FROM bp.date_start_time_match_utc_local) = 5 THEN 'friday'
        WHEN EXTRACT(DOW FROM bp.date_start_time_match_utc_local) = 6 THEN 'saturday'
    END AS day_of_week,
    CASE
        WHEN EXTRACT(HOUR FROM bp.date_start_time_match_utc_local) BETWEEN 6 AND 11 THEN 'morning'
        WHEN EXTRACT(HOUR FROM bp.date_start_time_match_utc_local) BETWEEN 12 AND 17 THEN 'afternoon'
        WHEN EXTRACT(HOUR FROM bp.date_start_time_match_utc_local) BETWEEN 18 AND 23 THEN 'evening'
        ELSE 'dawn'
    END AS period,
    CAST(bp.gtd_match AS INTEGER) AS gtd_match,
    ROUND(bp.buyin_value, 2) AS buyin_earned,
    ROUND(bp.rebuy_value, 2) AS rebuy_earned,
    ROUND(bp.addon_value, 2) AS addon_earned,
    bp.overlay_match,
    CASE
        WHEN bp.gtd_match != 0 THEN ABS((bp.overlay_match / bp.gtd_match) * 100)
        ELSE 0
    END AS overlay_perc,
    CASE
        WHEN bp.overlay_match < 0 THEN 0
        WHEN bp.overlay_match = 0 THEN 1
        ELSE bp.overlay_match
    END AS overlay_match_bi,
    bp.duration_match
FROM 
    poker_prod.poker.bi_product bp 
JOIN
    (SELECT DISTINCT id_match, value_buyin, value_rebuy, value_addon
    FROM poker_prod.poker.dim_union_summary) ds
    ON bp.id_match = ds.id_match
WHERE 
    bp.match_type_detail_name LIKE '{user_input['MATCH_TYPE_DETAIL_NAME'][0]}%'
    AND ds.value_buyin between {buyin_price_siml[0]} and {buyin_price_siml[1]}
    AND ds.value_rebuy between {rebuy_price_siml[0]} and {rebuy_price_siml[1]}
    AND ds.value_addon between {addon_price_siml[0]} and {addon_price_siml[1]}
    AND bp.gtd_match between {gtdmin_match_siml} and {gtdmax_match_siml}
    AND bp.class_match = '{user_input['CLASS_MATCH'][0]}'
    AND EXTRACT(DOW FROM bp.date_start_time_match_utc_local) = CASE 
        WHEN '{user_input['DAY_OF_WEEK'][0]}' = 'sunday' THEN 0
        WHEN '{user_input['DAY_OF_WEEK'][0]}' = 'monday' THEN 1
        WHEN '{user_input['DAY_OF_WEEK'][0]}' = 'tuesday' THEN 2
        WHEN '{user_input['DAY_OF_WEEK'][0]}' = 'wednesday' THEN 3
        WHEN '{user_input['DAY_OF_WEEK'][0]}' = 'thursday' THEN 4
        WHEN '{user_input['DAY_OF_WEEK'][0]}' = 'friday' THEN 5
        WHEN '{user_input['DAY_OF_WEEK'][0]}' = 'saturday' THEN 6
    END
    AND CASE
        WHEN '{user_input['PERIOD'][0]}' = 'morning' THEN EXTRACT(HOUR FROM bp.date_start_time_match_utc_local) BETWEEN 6 AND 11
        WHEN '{user_input['PERIOD'][0]}' = 'afternoon' THEN EXTRACT(HOUR FROM bp.date_start_time_match_utc_local) BETWEEN 12 AND 17
        WHEN '{user_input['PERIOD'][0]}' = 'evening' THEN EXTRACT(HOUR FROM bp.date_start_time_match_utc_local) BETWEEN 18 AND 23
        ELSE EXTRACT(HOUR FROM bp.date_start_time_match_utc_local) < 6
    END
    AND bp.duration_match > '00:05:00'
"""

# Coleta, transformacao e vizualizacao do dado coletado
similar_data = session.sql(query_sim).collect()
df_similar = pd.DataFrame(similar_data)

if expander_similar.button("Obter dados similares"):
    if df_similar.empty == True:
        expander_similar.write("Sem dados similares")
    else:
        expander_similar.write(df_similar)
############################################################################################################################################


######################################################### EXPANDER 2 #######################################################################

# Dado de importancia de cada feature
# Foi feita uma conversao em HTLM para alinhamento da pagina
expander = st.expander("Tabela de importância das features")
df_sorted = feat_importances.sort_values(by='Value', ascending=False)
table_html = df_sorted.to_html(index=False)
expander.markdown(f"""
    <div style="display: flex; justify-content: center;">
        {table_html}
    </div>
""", unsafe_allow_html=True)
#############################################################################################################################################
