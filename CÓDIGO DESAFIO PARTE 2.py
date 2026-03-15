import pandas as pd
import numpy as np
import glob
import matplotlib.pyplot as plt

def ler_e_limpar_dados(lista_arquivos):
    if not lista_arquivos:
        return pd.DataFrame()
    
    lista_df = []
    for arquivo in lista_arquivos:
        df = pd.read_csv(arquivo, skiprows=9, sep=',', parse_dates=['# Date and time'], low_memory=False)
        df = df.rename(columns={'# Date and time': 'Timestamp'})
        lista_df.append(df)
    
    return pd.concat(lista_df, ignore_index=True).sort_values(by='Timestamp')

def analisar_esteira(df_t2, df_t3):
    if df_t2.empty or df_t3.empty:
        print("Erro: Dataframes vazios.")
        return

    df_merge = pd.merge(df_t2, df_t3, on='Timestamp', suffixes=('_T2', '_T3'))

    col_dir, col_pitch, col_power, col_speed = 'Wind direction (°)', 'Blade angle (pitch position) A (°)', 'Power (kW)', 'Wind speed (m/s)'

    filtro = (df_merge[f'{col_dir}_T2'].between(167, 227)) & \
             (df_merge[f'{col_pitch}_T2'] <= 5) & (df_merge[f'{col_pitch}_T3'] <= 5) & \
             (df_merge[f'{col_power}_T2'] > 0) & (df_merge[f'{col_power}_T3'] > 0)

    df_filtrado = df_merge[filtro].copy()

    if df_filtrado.empty:
        print("Aviso: Nenhum dado restou após os filtros.")
        return

    df_filtrado['bin'] = (df_filtrado[f'{col_speed}_T2'] // 0.5) * 0.5
    curva_media = df_filtrado.groupby('bin')[[f'{col_power}_T2', f'{col_power}_T3']].mean().reset_index()

    plt.figure(figsize=(12, 6))
    plt.plot(curva_media['bin'], curva_media[f'{col_power}_T2'], 'o-', color='blue', label='T2 (Montante)')
    plt.plot(curva_media['bin'], curva_media[f'{col_power}_T3'], 's-', color='red', label='T3 (Jusante)')
    plt.fill_between(curva_media['bin'], curva_media[f'{col_power}_T2'], curva_media[f'{col_power}_T3'], color='gray', alpha=0.2)
    
    plt.title('Curvas de Potência Binarizadas: T2 vs T3 (Efeito de Esteira)')
    plt.xlabel('Velocidade do Vento (m/s)')
    plt.ylabel('Potência Ativa Média (kW)')
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.xlim(3, 15)
    
    plt.savefig('curva_esteira_T2_T3.png', dpi=300, bbox_inches='tight')
    plt.show()

    soma_t2 = curva_media[f'{col_power}_T2'].sum()
    soma_t3 = curva_media[f'{col_power}_T3'].sum()
    perda_percentual = (1 - (soma_t3 / soma_t2)) * 100 if soma_t2 > 0 else 0

    resultados = {
        "Pontos Sincronizados (N)": len(df_filtrado),
        "Direção Analisada": "197° (Sul-Sudoeste)",
        "Margem de Alinhamento": "±30°",
        "Soma Potência T2 (Bins)": f"{soma_t2:.2f} kW",
        "Soma Potência T3 (Bins)": f"{soma_t3:.2f} kW",
        "PERDA MÉDIA POR ESTEIRA": f"{perda_percentual:.2f}%"
    }

    largura = 55
    
    print("\n" + "=" * largura)
    print(f"{'RESULTADOS FINAIS - EFEITO ESTEIRA':^{largura}}")
    print("=" * largura)
    
    for chave, valor in resultados.items():
        print(f"{chave:<28} | {str(valor):>24}")
        
    print("=" * largura)
    print("\nArquivo gerado com sucesso: 'curva_esteira_T2_T3.png'")

todos_arquivos = glob.glob("Turbine_Data_*.csv")

arquivos_por_turbina = {
    "T2": [f for f in todos_arquivos if "Kelmarsh_2" in f],
    "T3": [f for f in todos_arquivos if "Kelmarsh_3" in f]
}

if not arquivos_por_turbina["T2"] or not arquivos_por_turbina["T3"]:
    print("Erro: Arquivos T2 ou T3 não encontrados na pasta atual.")
else:
    print("\npasso 1 - Lendo e limpando dados...")
    df_t2 = ler_e_limpar_dados(arquivos_por_turbina["T2"])
    df_t3 = ler_e_limpar_dados(arquivos_por_turbina["T3"])

    print("passo 2 - Executando análise e gerando resultados...")
    analisar_esteira(df_t2, df_t3)