# Verifica se um arquivo foi carregado
import streamlit as st
import pandas as pd
import joblib

# SET THE PATH FOR THE MODEL AND ENCODER:
path_encoder = '../new_model/encoder.pkl'
path_model = '../new_model/model.pkl'

encoder = joblib.load(path_encoder)
model = joblib.load(path_model)

# Título do App
st.title(':spades: :blue[Modelo de regressão MTT] :spades:')

data = st.file_uploader("Insira seu csv aqui")

if data is not None:
    data = pd.read_csv(data)
    data_transform = data.drop(columns=['MATCH_ID', 'START_DATE_LOCAL'])
    cat_columns = ['START_HOUR_LOCAL', 'DAY_OF_WEEK_LOCAL', 'KO_TYPE',
                   'MTT_POOL_ALLOCATION_DESCRIPTION', 'ESTRUTURA_BLINDS']
    encoded_columns = encoder.fit_transform(data_transform[cat_columns])
    encoded_df = pd.DataFrame(encoded_columns, columns=encoder.get_feature_names_out(cat_columns))
    data_final = pd.concat([data_transform.drop(cat_columns, axis=1), encoded_df], axis=1)

    # FITTING COLUMNS LIKE THE TRAINED MODEL
    columns = list(model.feature_names_in_)
    new_final = data_final.reindex(columns=columns, fill_value=0)

    # EXECUTING THE MODEL
    predictions = model.predict(new_final)

    # COPY FROM ORIGINAL DF, OBTAINING THE RESULT, AND CREATION OF THE BOOL COLUMN "HAS_OVERLAY"
    data_teste = data
    data_teste['Predictions'] = predictions
    data_teste['result_pred'] = data_teste['Predictions'] - data_teste['GTD']

    def check_overlay(dataframe):
        dataframe['has_overlay_pred'] = [True if value < 0 else False for value in dataframe['result_pred']]
        return dataframe

    data_teste = check_overlay(data_teste)

    @st.cache_data
    def convert_df(df):
       return df.to_csv(index=False).encode('utf-8')

    csv = convert_df(data_teste)

    # Botao para aplicaco do modelo
    st.download_button(
       "Press to Download",
       csv,
       "file.csv",
       "text/csv",
       key='download-csv'
    )
else:
    st.write("Por favor, faça o upload de um arquivo CSV para continuar.")
