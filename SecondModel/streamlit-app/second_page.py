import streamlit as st
import pandas as pd
import joblib
import pickle
import cachetools
#from snowflake.snowpark.context import get_active_session

#session = get_active_session()


# Funcoes para ler arquivos do stage
'''@cachetools.cached(cache={})
def read_model():
    with session.file.get_stream('@"STREAMLIT"."DEV"."STREAMLIT_STAGE"/staged/best_model_no_players.pkl') as file:
        m = joblib.load(file)
        return m


def read_encoder():
    with session.file.get_stream('@"STREAMLIT"."DEV"."STREAMLIT_STAGE"/staged/new_encoder.pkl') as file:
        m = joblib.load(file)
        return m'''

path_model = 'model.pkl'
path_encoder = 'encoder.pkl'

with open(path_encoder, 'rb') as f:
    encoder = pickle.load(f)


with open(path_model, 'rb') as f:
    model = pickle.load(f)


def generate_hour_variables(hour_input):
    # Inicializa todas as variáveis como False
    variables = [False] * 24

    # Define a variável correspondente ao input como True
    if 0 <= hour_input < 24:  # Garante que o input está dentro do intervalo
        variables[int(hour_input)] = True

    # Retorna as variáveis separadamente
    return tuple(variables)

#model = read_model()
#encoder = read_encoder()

# Título do App
st.title(':spades: :blue[Modelo de previsão MTT] :spades:')

# Valores numericos de input para o modelo
level_reject = st.number_input('Level reject', min_value=0.0, format="%.2f", key='lvl_reject')
mtt_blind_up_time_sec = st.number_input('Blinds up em segundos', min_value=0.0, format="%.2f", key='blind_up')
late_registration_time = mtt_blind_up_time_sec * level_reject
earlybird = st.number_input('Bonus EarlyBird (20% = 0.2)', min_value=0.0, format="%.2f", key='earlybird')
has_rebuy = st.selectbox("Possui Rebuy?", ('Sim', 'Nao'))

if has_rebuy == 'Sim':
    has_rebuy = 'True'
else:
    has_rebuy = 'False'

has_addon = st.selectbox("Possui Addon?", ('Sim', 'Nao'))
if has_addon == 'Sim':
    has_addon = 'True'
else:
    has_addon = 'False'


mtt_customer_buy_in = st.number_input('Valor do Buyin em dolares', min_value=0.1, format="%.2f", key='buyin')
mtt_customer_rebuy = st.number_input('Valor do Rebuy em dolares', min_value=0.0, format="%.2f", key='rebuy')
mtt_customer_addon = st.number_input('Valor do Addon em dolares', min_value=0.0, format="%.2f", key='addon')
gtd = st.number_input('Valor Garantido em dolares', min_value=1.0, format="%.2f", key='gtd')
buyin_gtd_rate = gtd / mtt_customer_buy_in
same_hour_game = 'TESTE'
one_hour_window_game = ' TESTE'

hour_input = st.number_input('Hora do torneio', min_value=0.0, max_value=23.99, format="%.2f", key='hour')
(
    START_HOUR_LOCAL_0, START_HOUR_LOCAL_1, START_HOUR_LOCAL_2, START_HOUR_LOCAL_3, START_HOUR_LOCAL_4,
    START_HOUR_LOCAL_5, START_HOUR_LOCAL_6, START_HOUR_LOCAL_7, START_HOUR_LOCAL_8, START_HOUR_LOCAL_9,
    START_HOUR_LOCAL_10, START_HOUR_LOCAL_11, START_HOUR_LOCAL_12, START_HOUR_LOCAL_13, START_HOUR_LOCAL_14,
    START_HOUR_LOCAL_15, START_HOUR_LOCAL_16, START_HOUR_LOCAL_17, START_HOUR_LOCAL_18, START_HOUR_LOCAL_19,
    START_HOUR_LOCAL_20, START_HOUR_LOCAL_21, START_HOUR_LOCAL_22, START_HOUR_LOCAL_23
) = generate_hour_variables(hour_input)


dias_semana = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
dayofweek = st.selectbox("Qual o dia da semana?", dias_semana)
# Inicializa o dicionário com todas as variáveis como False
day_of_week_variables = {f'DAY_OF_WEEK_LOCAL_{dia}': False for dia in dias_semana}
# Define a variável correspondente ao dia selecionado como True
day_of_week_variables[f'DAY_OF_WEEK_LOCAL_{dayofweek}'] = True
# Desempacota as variáveis do dicionário
DAY_OF_WEEK_LOCAL_Domingo = day_of_week_variables['DAY_OF_WEEK_LOCAL_Domingo']
DAY_OF_WEEK_LOCAL_Segunda = day_of_week_variables['DAY_OF_WEEK_LOCAL_Segunda']
DAY_OF_WEEK_LOCAL_Terça = day_of_week_variables['DAY_OF_WEEK_LOCAL_Terça']
DAY_OF_WEEK_LOCAL_Quarta = day_of_week_variables['DAY_OF_WEEK_LOCAL_Quarta']
DAY_OF_WEEK_LOCAL_Quinta = day_of_week_variables['DAY_OF_WEEK_LOCAL_Quinta']
DAY_OF_WEEK_LOCAL_Sexta = day_of_week_variables['DAY_OF_WEEK_LOCAL_Sexta']
DAY_OF_WEEK_LOCAL_Sábado = day_of_week_variables['DAY_OF_WEEK_LOCAL_Sábado']


ko_type = ['KO', 'Mystery Bounty', 'Progressive KO', 'Regular']
KO = st.selectbox("Selecione o tipo de KO", ko_type)
ko_type_var = {f'KO_TYPE_{types}': False for types in ko_type}
ko_type_var[f'KO_TYPE_{KO}'] = True
KO_TYPE_KO = ko_type_var['KO_TYPE_KO']
KO_TYPE_Mystery_Bounty = ko_type_var[f'KO_TYPE_Mystery Bounty'] # aqui deve tirar o _ do bounty
KO_TYPE_Progressive_KO = ko_type_var[f'KO_TYPE_Progressive KO'] # same thing
KO_TYPE_Regular = ko_type_var[f'KO_TYPE_Regular']

pool = ['-', '10%', '10% Flat', '10% Plus', '15%', '15% Flat', '20%', '20% Flat']
pool_aloc = st.selectbox("Selecione o tipo de POOL", pool)
pool_aloc_var ={f'MTT_POOL_ALLOCATION_DESCRIPTION_{allocation}': False for allocation in pool}
pool_aloc_var[f'MTT_POOL_ALLOCATION_DESCRIPTION_{pool_aloc}'] = True
mtt_pool_allocation_description_ = pool_aloc_var['MTT_POOL_ALLOCATION_DESCRIPTION_-']
mtt_pool_allocation_description_10 = pool_aloc_var['MTT_POOL_ALLOCATION_DESCRIPTION_10%']
mtt_pool_allocation_description_10_flat = pool_aloc_var['MTT_POOL_ALLOCATION_DESCRIPTION_10% Flat']
mtt_pool_allocation_description_10_plus = pool_aloc_var['MTT_POOL_ALLOCATION_DESCRIPTION_10% Plus']
mtt_pool_allocation_description_15 = pool_aloc_var['MTT_POOL_ALLOCATION_DESCRIPTION_15%']
mtt_pool_allocation_description_15_flat = pool_aloc_var['MTT_POOL_ALLOCATION_DESCRIPTION_15% Flat']
mtt_pool_allocation_description_20 = pool_aloc_var['MTT_POOL_ALLOCATION_DESCRIPTION_20%']
mtt_pool_allocation_description_20_flat = pool_aloc_var['MTT_POOL_ALLOCATION_DESCRIPTION_20% Flat']

blinds = ['Hyper', 'Hyper - no ante', 'Standard - no ante', 'Turbo', 'Turbo - no ante']
struct_blinds = st.selectbox("Selecione a estrutura de blinds", blinds)
struct_blinds_var ={f'ESTRUTURA_BLINDS_{blind}': False for blind in blinds}
struct_blinds_var[f'ESTRUTURA_BLINDS_{struct_blinds}'] = True

estrut_blinds_hyper = struct_blinds_var['ESTRUTURA_BLINDS_Hyper']
estrut_blinds_hyper_no_ante = struct_blinds_var['ESTRUTURA_BLINDS_Hyper - no ante']
estrut_blinds_standard_no_ante = struct_blinds_var['ESTRUTURA_BLINDS_Standard - no ante']
estrut_blinds_turbo = struct_blinds_var['ESTRUTURA_BLINDS_Turbo']
estrut_blinds_turbo_no_ante = struct_blinds_var['ESTRUTURA_BLINDS_Turbo - no ante']

# Dicionario com todos os dados brutos do usuario
user_input = {
    'LEVEL_REJECT':[level_reject],
    'MTT_BLIND_UP_TIME_SEC':[mtt_blind_up_time_sec],
    'LATE_REGISTRATION_TIME':[late_registration_time],
    'EARLYBIRD':[earlybird],
    'HAS_REBUY':[has_rebuy],
    'HAS_ADDON':[has_addon],
    'MTT_CUSTOMER_BUY_IN':[mtt_customer_buy_in],
    'MTT_CUSTOMER_REBUY':[mtt_customer_rebuy],
    'MTT_CUSTOMER_ADDON':[mtt_customer_addon],
    'GTD':[gtd],
    'BUYIN_GTD_RATE':[buyin_gtd_rate],
    'SAME_HOUR_GAME':[same_hour_game],
    'ONE_HOUR_WINDOW_GAME':[one_hour_window_game],
    'START_HOUR_LOCAL_0':[START_HOUR_LOCAL_0],
    'START_HOUR_LOCAL_1':[START_HOUR_LOCAL_1],
    'START_HOUR_LOCAL_2':[START_HOUR_LOCAL_2],
    'START_HOUR_LOCAL_3':[START_HOUR_LOCAL_3],
    'START_HOUR_LOCAL_4':[START_HOUR_LOCAL_4],
    'START_HOUR_LOCAL_5':[START_HOUR_LOCAL_5],
    'START_HOUR_LOCAL_6':[START_HOUR_LOCAL_6],
    'START_HOUR_LOCAL_7':[START_HOUR_LOCAL_7],
    'START_HOUR_LOCAL_8':[START_HOUR_LOCAL_8],
    'START_HOUR_LOCAL_9':[START_HOUR_LOCAL_9],
    'START_HOUR_LOCAL_10':[START_HOUR_LOCAL_10],
    'START_HOUR_LOCAL_11':[START_HOUR_LOCAL_11],
    'START_HOUR_LOCAL_12':[START_HOUR_LOCAL_12],
    'START_HOUR_LOCAL_13':[START_HOUR_LOCAL_13],
    'START_HOUR_LOCAL_14':[START_HOUR_LOCAL_14],
    'START_HOUR_LOCAL_15':[START_HOUR_LOCAL_15],
    'START_HOUR_LOCAL_16':[START_HOUR_LOCAL_16],
    'START_HOUR_LOCAL_17':[START_HOUR_LOCAL_17],
    'START_HOUR_LOCAL_18':[START_HOUR_LOCAL_18],
    'START_HOUR_LOCAL_19':[START_HOUR_LOCAL_19],
    'START_HOUR_LOCAL_20':[START_HOUR_LOCAL_20],
    'START_HOUR_LOCAL_21':[START_HOUR_LOCAL_21],
    'START_HOUR_LOCAL_22':[START_HOUR_LOCAL_22],
    'START_HOUR_LOCAL_23':[START_HOUR_LOCAL_23],
    'DAY_OF_WEEK_LOCAL_Domingo':[DAY_OF_WEEK_LOCAL_Domingo],
    'DAY_OF_WEEK_LOCAL_Quarta':[DAY_OF_WEEK_LOCAL_Quarta],
    'DAY_OF_WEEK_LOCAL_Quinta':[DAY_OF_WEEK_LOCAL_Quinta],
    'DAY_OF_WEEK_LOCAL_Segunda':[DAY_OF_WEEK_LOCAL_Segunda],
    'DAY_OF_WEEK_LOCAL_Sexta':[DAY_OF_WEEK_LOCAL_Sexta],
    'DAY_OF_WEEK_LOCAL_Sábado':[DAY_OF_WEEK_LOCAL_Sábado],
    'DAY_OF_WEEK_LOCAL_Terça':[DAY_OF_WEEK_LOCAL_Terça],
    'KO_TYPE_KO':[KO_TYPE_KO],
    'KO_TYPE_Mystery Bounty':[KO_TYPE_Mystery_Bounty],
    'KO_TYPE_Progressive KO':[KO_TYPE_Progressive_KO],
    'KO_TYPE_Regular':[KO_TYPE_Regular],
    'MTT_POOL_ALLOCATION_DESCRIPTION_-':[mtt_pool_allocation_description_],
    'MTT_POOL_ALLOCATION_DESCRIPTION_10%':[mtt_pool_allocation_description_10],
    'MTT_POOL_ALLOCATION_DESCRIPTION_10% Flat':[mtt_pool_allocation_description_10_flat],
    'MTT_POOL_ALLOCATION_DESCRIPTION_10% Plus':[mtt_pool_allocation_description_10_plus],
    'MTT_POOL_ALLOCATION_DESCRIPTION_15%':[mtt_pool_allocation_description_15],
    'MTT_POOL_ALLOCATION_DESCRIPTION_15% Flat':[mtt_pool_allocation_description_15_flat],
    'MTT_POOL_ALLOCATION_DESCRIPTION_20%':[mtt_pool_allocation_description_20],
    'MTT_POOL_ALLOCATION_DESCRIPTION_20% Flat':[mtt_pool_allocation_description_20_flat],
    'ESTRUTURA_BLINDS_Hyper':[estrut_blinds_hyper],
    'ESTRUTURA_BLINDS_Hyper - no ante':[estrut_blinds_hyper],
    'ESTRUTURA_BLINDS_Standard - no ante':[estrut_blinds_standard_no_ante],
    'ESTRUTURA_BLINDS_Turbo':[estrut_blinds_turbo],
    'ESTRUTURA_BLINDS_Turbo - no ante':[estrut_blinds_turbo]
}

"""df_user_input = pd.DataFrame(user_input)

# Separacao de colunas categoricas e numericas
categorical_columns = ['MATCH_TYPE_DETAIL_NAME', 'CLASS_MATCH', 'DAY_OF_WEEK', 'PERIOD']
numerical_columns = ['BUYIN_PRICE', 'REBUY_PRICE', 'ADDON_PRICE', 'GTD_MATCH']

# Aplicacao do OnohotEncoder nos dados categoricos
df_categorical = df_user_input[categorical_columns]
encoded_input = encoder.transform(df_categorical)
encoded_input_df = pd.DataFrame(encoded_input, columns=encoder.get_feature_names_out(categorical_columns))

# Concat dos dois tipos de dados novamente
df_numerical = df_user_input[numerical_columns].reset_index(drop=True)
final_input_df = pd.concat([df_numerical, encoded_input_df], axis=1)"""

# Garantindo as colunas esperadas pelo modelo em ordem, caso nao tenha valor o 0 e adicionado
'''expected_columns = model.get_booster().feature_names
for col in expected_columns:
    if col not in final_input_df.columns:
        final_input_df[col] = 0
final_input_df = final_input_df[expected_columns]'''

'''# Botao para aplicaco do modelo
if st.button('Fazer Previsão e extrair dados iguais'):
    predicao = model.predict(final_input_df)
    if predicao[0] == 1:
        st.write('PREVISÃO DE LUCRO')
    else:
        st.write('PREVISÃO DE PREJUIZO')

    # Consulta SQL construída dinamicamente



    st.subheader('Dados iguais no banco:')


    if df_equal.empty == True:
        st.write('Sem dados similares')
    else:
        st.write(df_equal)
st.subheader("", divider='blue')
'''

############################################################################################################################################
