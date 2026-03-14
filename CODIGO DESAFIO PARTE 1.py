import pandas as pd 
import glob 

def ler_e_limpar_status(lista_arquivos): 
    if not lista_arquivos:
        return pd.DataFrame()  
    
    lista_df = [] 

    for arquivo in lista_arquivos:
        df = pd.read_csv(arquivo, 
                         skiprows=9,  
                         sep=',',     
                         parse_dates=['Timestamp start', 'Timestamp end'],  
                         dayfirst=False)  
        lista_df.append(df)               
    
    df_consolidado = pd.concat(lista_df, ignore_index=True)
    df_consolidado = df_consolidado.sort_values(by='Timestamp start')
    
    return df_consolidado

def preparar_para_analise(df): 
    if df.empty:
        return df
    
    df['Duration_td'] = pd.to_timedelta(df['Duration'], errors='coerce')
    df['Duration_sec'] = df['Duration_td'].dt.total_seconds()
    df['Ano'] = df['Timestamp start'].dt.year
    
    return df

def executar_analise_precisa(df, nome):
    if df.empty:
        return
    
    print(f"\nANÁLISE {nome}:")
    
    status_de_parada = ['Stop'] 
    df_falhas = df[df['Status'].isin(status_de_parada)].copy()
    
    mensagens_ignoradas = ['Data communication unavailable', 'Icing (anemometer)']
    df_falhas = df_falhas[~df_falhas['Message'].isin(mensagens_ignoradas)]
    
    df_falhas = df_falhas.sort_values('Duration_sec', ascending=False).drop_duplicates(subset=['Timestamp start'])

    segundos_totais_ano = {2019: 31536000, 2020: 31622400, 2021: 31536000}
    
    anos_presentes = df_falhas['Ano'].dropna().unique()
    for ano in sorted(anos_presentes):
        df_ano = df_falhas[df_falhas['Ano'] == ano]
        tempo_parado = df_ano['Duration_sec'].sum()
        total_seg = segundos_totais_ano.get(ano, 31536000)
        
        disponibilidade = ((total_seg - tempo_parado) / total_seg) * 100
        print(f"Disponibilidade em {int(ano)}: {disponibilidade:.2f}%")

    top = df_falhas.groupby('Message')['Duration_sec'].sum().sort_values(ascending=False).head(3)

    print(f"\nTop 3 causas reais (justificativa das paradas):")
    for causa, seg in top.items():
        print(f"- {causa}: {seg/3600:.2f} horas")


todos_arquivos = glob.glob("Status_Kelmarsh_*.csv")

arquivos_por_turbina = {f"T{i}": [f for f in todos_arquivos if f"Kelmarsh_{i}" in f] for i in range(1, 7)}

print("\npasso 1 - listagem dos arquivos por turbina:")
for nome, arquivos in arquivos_por_turbina.items():
    print(f"Arquivos {nome} encontrados: {arquivos}")

dados_turbinas = {}

print("\npasso 2 e 3 - leitura, limpeza e preparação dos dados.")
for nome, arquivos in arquivos_por_turbina.items():
    df = ler_e_limpar_status(arquivos)
    df = preparar_para_analise(df)
    
    dados_turbinas[nome] = df
    
    if not df.empty: 
        anos_presentes = df['Ano'].unique()
        soma_segundos = df['Duration_sec'].sum()
        
        print(f"\nTurbina {nome}:")
        print(f"\tAnos encontrados: {anos_presentes}")
        print(f"\tTotal de segundos processados: {soma_segundos:.0f}s")
        print(df[['Timestamp start', 'Ano', 'Duration', 'Duration_sec']].head(2))
        print("-" * 30) 
    else:
        print(f"\nTurbina {nome}: Sem dados carregados.")

for nome, df in dados_turbinas.items():
    executar_analise_precisa(df, nome)
