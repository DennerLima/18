import streamlit as st
from dotenv import load_dotenv
import pandas as pd
import plotly.express as px
import random
import os
from datetime import datetime, timedelta
import psycopg2
from sqlalchemy import create_engine

class DbSupabase:
    def __init__(self):
        self.conexao = None
        self.engine = None
        self.openSupabase()

    def openSupabase(self):
        try:
            self.conexao = psycopg2.connect(os.getenv("CONEXAO"))
            self.engine = create_engine('postgresql+psycopg2://', creator=lambda: self.conexao)
        except Exception as e:
            print(f"Erro ao conectar ao banco de dados: {e}")

    def closeSupabase(self):
        if self.conexao:
            self.conexao.close()
            self.conexao = None
            self.engine = None

def fazer_consulta(TIPO, start_date, end_date):
    Supabase = DbSupabase()
    if TIPO == "Controle de Estoque":
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d') + " 23:59:59"
        company = os.getenv('COMPANY')
        sql = f""" 
        WITH tarefas_filtradas AS (
            SELECT 
                idtarefa,
                TO_TIMESTAMP(dataconclusao, 'DD/MM/YYYY HH24:MI:SS')::DATE AS dataconclusao,
                tecnico
            FROM 
                tarefas
            WHERE 
                TO_TIMESTAMP(dataconclusao, 'DD/MM/YYYY HH24:MI:SS') BETWEEN '{start_date_str}' AND '{end_date_str}'
                AND company =  '{company}'
                AND atividade = 'ativ_separacaoeentregamaterial'
        ),
        solicitantes AS (
            SELECT 
                idtarefa,
                TRIM(SPLIT_PART(valor, 'Solicitado por: ', 2)) AS solicitante
            FROM 
                atividades
            WHERE 
                secao = 'Entrega e assinatura do solicitante'
                AND campo = 'solicitante'
                AND company =  '{company}'
                AND descricao_atividade = 'ativ_separacaoeentregamaterial'        
        )
        SELECT 
            atividades.item,
            tarefas_filtradas.dataconclusao,
            solicitantes.solicitante,
            atividades.campo,
            atividades.valor
        FROM 
            atividades
        JOIN 
            tarefas_filtradas 
        ON 
            atividades.idtarefa = tarefas_filtradas.idtarefa
        LEFT JOIN 
            solicitantes
        ON 
            atividades.idtarefa = solicitantes.idtarefa
        WHERE 
            atividades.campo IN ('Quantidade Separada', 'valorunidadeqtd')
            AND atividades.company =  '{company}';
        """

        df = pd.read_sql_query(sql, Supabase.conexao)
        df = df.groupby(['item', 'dataconclusao', 'solicitante', 'campo'])['valor'].first().unstack()
        df.reset_index(inplace=True)
        df.index.name = None
        df = df.dropna(axis=1, how='all')
        df['Quantidade Separada'] = pd.to_numeric(df['Quantidade Separada'], errors='coerce')
        df['valorunidadeqtd'] = pd.to_numeric(df['valorunidadeqtd'], errors='coerce')
        df['dataconclusao'] = pd.to_datetime(df['dataconclusao'], format="%d/%m/%Y")
        df = df[(df['dataconclusao'] >= pd.to_datetime(start_date)) & (df['dataconclusao'] <= pd.to_datetime(end_date))]
        if Supabase.conexao:
            Supabase.closeSupabase()
        return df
    else:
        # Função para gerar uma lista de datas aleatórias
        def random_date(start_date, end_date):
            delta = end_date - start_date
            random_days = random.randint(0, delta.days)
            return start_date + timedelta(days=random_days)

        # Função para gerar o DataFrame aleatório
        def generate_data():
            # Dados de exemplo
            num_records = 30  # Quantidade máxima de itens

            # Gerar nomes aleatórios para itens
            items = [
                'ABRAÇADEIRA G', 'ABRAÇADEIRA GG', 'ABRAÇADEIRA M', 'ALÇA 24FO', 'ALÇA CABO 48',
                'AX2', 'BRID', 'BRID TELEFONIA', 'CABO 24FO', 'CONECTOR X', 'CABO DE REDE CAT6', 
                'CABO DE FIBRA', 'PLUG RJ45', 'EXTENSÃO 10M', 'PARAFUSO', 'LUVA PROTETORA',
                'CHAVE DE FENDA', 'BROCA AÇO', 'ADESIVO INSTANTÂNEO', 'CAIXA DE FERRAMENTA',
                'FITA ISOLANTE', 'LANTERNA LED', 'MOUSE PAD', 'TECLADO MECÂNICO', 'CABO USB',
                'MULTÍMETRO', 'ALICATE DE CORTE', 'SENSOR DE TEMPERATURA', 'DISJUNTOR', 'LUVAS'
            ]

            solicitantes = ['solicitante_1','solicitante_2','solicitante_3','solicitante_4','solicitante_5','solicitante_6','solicitante_7',
                            'solicitante_8','solicitante_9','solicitante_10','solicitante_11','solicitante_12','solicitante_13','solicitante_14',
                            'solicitante_15','solicitante_16','solicitante_17','solicitante_18','solicitante_19','solicitante_20']

            # Criar DataFrame aleatório
            data = {
                'item': [random.choice(items) for _ in range(num_records)],
                'dataconclusao': [random_date(datetime(2024, 10, 1), datetime(2024, 11, 18)).strftime('%d/%m/%Y') for _ in range(num_records)],
                'solicitante': [random.choice(solicitantes) for _ in range(num_records)],
                'Quantidade Separada': [random.randint(1, 10) for _ in range(num_records)],
                'valorunidadeqtd': [round(random.uniform(0.01, 321.45), 2) for _ in range(num_records)]
            }
            df = pd.DataFrame(data)
            return df
        df = generate_data()
        df['dataconclusao'] = pd.to_datetime(df['dataconclusao'], format="%d/%m/%Y")
        df = df[(df['dataconclusao'] >= pd.to_datetime(start_date)) & (df['dataconclusao'] <= pd.to_datetime(end_date))]
        return df

st.set_page_config(
    page_title="Controle de Materiais",
    layout="centered",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://www.extremelycoolapp.com/help'
    }
)

if 'resultados' not in st.session_state:
    st.session_state.resultados = []

#st.sidebar.image("logo2.png")

TIPO = st.sidebar.selectbox(
    "Selecione o dashboard:",
    ["Controle de Estoque"],
    index=0
)
#TIPO = st.sidebar.selectbox(
#    "Selecione o dashboard:",
#    ["Controle de Estoque REAL", "Controle de Estoque ficticio"],
#    index=0
#)


st.sidebar.markdown ("---")
today = datetime.today()
last_30_days = today - timedelta(days=30)

# Barra lateral para filtros com valores padrão
start_date = st.sidebar.date_input("Data Início", last_30_days.date(), format="DD/MM/YYYY")
end_date = st.sidebar.date_input("Data Fim", today.date(), format="DD/MM/YYYY")

df = fazer_consulta(TIPO, start_date, end_date)
st.session_state.resultados = df

# Botão para atualizar os dados
if st.sidebar.button('Atualizar Dados', key="atualizar_dados"):
    df = fazer_consulta(TIPO, start_date, end_date)
    st.session_state.resultados = df

if df.empty:
    st.text ("No período informado não há informações!")
else:
    if "Controle de Estoque" in TIPO:
        # Totalizadores
        total_quantidade = df['Quantidade Separada'].sum()
        total_valor = (df['Quantidade Separada'] * df['valorunidadeqtd']).sum()
        col0, col1, col2, col3 = st.columns(4)
        col0.write('')
        with col1:
            st.metric(label="Total em Unidades", value=total_quantidade, delta_color='inverse')
        with col2:
            st.metric(label="Total em R$", value=f"{total_valor:,.2f}", delta_color='inverse')
        col3.write('')

        tab1, tab2, tab3 = st.tabs(["Quantitativo por Item", "Valor Total por Solicitante", "Dados"])
        with tab1:
            # Gráfico 1: Quantitativo por Item (decrescente)
            df_grouped = df.groupby('item', as_index=False)['Quantidade Separada'].sum()
            fig1 = px.bar(df_grouped.sort_values(by='Quantidade Separada', ascending=False), 
                        x='item', y='Quantidade Separada', 
                        title="Quantitativo por Item Entregue", text_auto=True,
                        color_discrete_sequence=['#DA301D'])
            st.plotly_chart(fig1)

        with tab2:
            # Gráfico 2: Demonstrativo de Fluxo de Caixa (Valor Total por Item) (decrescente)
            df['Valor Total'] = df['Quantidade Separada'] * df['valorunidadeqtd']
            df_grouped1 = df.groupby('solicitante', as_index=False)['Valor Total'].sum()
            fig2 = px.bar(df_grouped1.sort_values(by='Valor Total', ascending=False), 
                        x='solicitante', y='Valor Total', 
                        title="Demonstrativo de Fluxo de Caixa (DFC)", text_auto=True,
                        color_discrete_sequence=['#DA301D'])
            st.plotly_chart(fig2)

        with tab3:

            df_draft = df.drop(columns=['dataconclusao', 'valorunidadeqtd'])
            df_draft = df_draft.groupby(['item', 'solicitante'], as_index=False)['Valor Total'].sum()
            df_draft = df_draft[['solicitante', 'item', 'Valor Total']]
            df_draft = df_draft.sort_values(by='solicitante', ascending=True)     
            df_draft = df_draft.reset_index(drop=True)   
            st.dataframe(df_draft)
            st.dataframe(df)