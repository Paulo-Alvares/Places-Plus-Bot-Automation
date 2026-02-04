import pandas as pd
from google.cloud import bigquery
from playwright.sync_api import sync_playwright
from datetime import datetime
import time
import os
import config

# --- ETAPA 1: DADOS DO PLACES+ ---
def etapa_1_baixar_base_places():
    print("--- Iniciando Etapa 1: Baixar base atual do Places+ ---")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        print("Acessando site...")
        page.goto("https://beneficiosplaces.gointegro.com/adminpanel/home")
        
        page.fill('input[type="text"]', config.EMAIL_PLACES)
        page.fill('input[type="password"]', config.SENHA_PLACES)
        page.click('button:has-text("Iniciar sessão")') 
        
        print("Navegando para Gerenciar Pessoas...")
        page.wait_for_selector('text="Pessoas"', timeout=15000)
        page.click('text="Pessoas"')
        page.wait_for_selector('text="Gerenciar"', timeout=15000)
        page.click('text="Gerenciar"')

        page.wait_for_selector('table.table', timeout=15000)
        time.sleep(3)

        print("Iniciando download...")
        print("Clicando no checkbox mestre...")
        # Clica na label do checkbox (force=True para garantir o clique)
        page.locator("table thead th label").first.click(force=True)
        time.sleep(2)
        
        print("Expandindo seleção para todos...")
        try:
            page.wait_for_selector('text="Selecionar todas"', timeout=5000)
            page.click('text="Selecionar todas"')
        except:
            print("Aviso: Botão 'Selecionar todas' não apareceu.")
        
        print("Baixando arquivo...")
        with page.expect_download() as download_info:
            page.click('.btndownload') 
        
        download = download_info.value
        download.save_as(config.ARQUIVO_BASE_PLACES)
        print(f"Arquivo baixado com sucesso: {config.ARQUIVO_BASE_PLACES}")
        
        browser.close()

    return pd.read_csv(config.ARQUIVO_BASE_PLACES)

# --- ETAPA 2: DADOS DO BIGQUERY ---
def etapa_2_buscar_bigquery():
    print("--- Iniciando Etapa 2: Obtenção de Dados ---")
    
    # Plano A: CSV Manual (Caso o BQ falhe)
    arquivo_manual = "base_bq.csv"
    if os.path.exists(arquivo_manual):
        print(f"📂 Arquivo local '{arquivo_manual}' encontrado! Usando modo offline.")
        df = pd.read_csv(arquivo_manual, dtype=str) 
        df.rename(columns={'SHP_AGENCY_ID': 'ID', 'SHP_AGEN_BUSINESS_NAME': 'Nome', 
                           'SHP_AGEN_STATUS': 'SBO', 'SHP_SITE_ID': 'Pais_Code'}, inplace=True)
        return df

    # Plano B: API BigQuery
    max_tentativas = 5
    client = bigquery.Client(project='meli-bi-data') # Tente usar o ID do projeto correto aqui
    
    query = r"""
    SELECT
      CAST(SHP_AGENCY_ID AS STRING) AS ID,
      SHP_AGEN_BUSINESS_NAME AS Nome,    
      SHP_AGEN_STATUS AS SBO,
      SHP_SITE_ID AS Pais_Code
    FROM `meli-bi-data.WHOWNER.LK_SHP_AGENCIES_API`
    WHERE
      SHP_CARRIER_ID IN (849817033, 1703373469, 1313953487, 17243954, 3377270)
      AND REGEXP_CONTAINS(TRIM(SHP_AGENCY_ID), r'^\d+$')
      AND LEFT(SHP_AGENCY_ID,1) NOT IN ("T",'t','0', 'N')
      AND SAFE_CAST(SHP_AGENCY_ID AS INT64) IS NOT NULL
      AND SHP_AGEN_CS_INSTALLATION_AVAILABILITY_FLAG IS FALSE
      AND SHP_AGEN_STATUS IN ('active', 'inactive')
    """
    
    for tentativa in range(1, max_tentativas + 1):
        try:
            print(f"🔄 Tentativa {tentativa}/{max_tentativas} no BigQuery...")
            df = client.query(query).to_dataframe()
            print(f"✅ Sucesso! Dados obtidos: {len(df)} linhas.")
            return df
        except Exception as e:
            print(f"⚠️ Falha na tentativa {tentativa}: {e}")
            if "db-dtypes" in str(e):
                print("❌ Instale a lib: pip install db-dtypes")
                exit()
            time.sleep(10)
            if tentativa == max_tentativas: raise e

# --- ETAPA 3: PROCESSAMENTO (O FAROL) ---
def etapa_3_processamento(df_bq, df_places):
    print("--- Iniciando Etapa 3: Cruzamento de Dados (O Farol) ---")
    
    # Limpeza
    df_bq['ID'] = df_bq['ID'].astype(str).str.strip()
    col_id_places = 'first_name' 
    df_places[col_id_places] = df_places[col_id_places].astype(str).str.strip()
    
    # Remove duplicatas e cria índices
    df_places.drop_duplicates(subset=[col_id_places], keep='first', inplace=True)
    ids_no_places = set(df_places[col_id_places])
    mapa_status_places = dict(zip(df_places[col_id_places], df_places['status']))
    df_places_indexado = df_places.set_index(col_id_places)
    
    lista_inclusao = []
    lista_exclusao = []

    for index, row in df_bq.iterrows():
        id_user = row['ID']
        status_sbo = row['SBO']
        pais_code = row['Pais_Code']
        
        esta_no_places = id_user in ids_no_places
        info_pais = config.MAPA_PAISES.get(pais_code)
        
        if not info_pais: continue 
            
        # Regra Inclusão
        if status_sbo == 'active' and not esta_no_places:
            lista_inclusao.append({
                "first_name": id_user,
                "last_name": "PLACE",
                "employee_id": id_user,
                "groups": info_pais['group'],
                "country": info_pais['country']
            })
            
        # Regra Exclusão
        elif status_sbo == 'inactive' and esta_no_places:
            if mapa_status_places.get(id_user) != 'active': continue
            
            dados_usuario = df_places_indexado.loc[id_user]
            email_usuario = str(dados_usuario.get('email', '')).lower()
            
            if '@mercadolivre' in email_usuario or '@mercadolibre' in email_usuario: continue

            lista_exclusao.append({
                "first_name": id_user,
                "last_name": "PLACE", 
                "status": "inactive",
                "email": dados_usuario.get('email', ''),
                "employee_id": id_user,
                "groups": dados_usuario.get('groups', ''),
                "country": dados_usuario.get('country', '')
            })

    df_incluir = pd.DataFrame(lista_inclusao)
    df_excluir = pd.DataFrame(lista_exclusao)
    
    # Salva Arquivos de Upload
    if not df_incluir.empty:
        df_incluir.to_csv(config.ARQUIVO_INCLUSAO, index=False)
        print(f"✅ Arquivo de INCLUSÃO gerado: {len(df_incluir)} usuários.")
    else:
        print("ℹ️ Ninguém para incluir hoje.")

    if not df_excluir.empty:
        df_excluir.to_csv(config.ARQUIVO_EXCLUSAO, index=False)
        print(f"✅ Arquivo de EXCLUSÃO gerado: {len(df_excluir)} usuários.")
    else:
        print("ℹ️ Ninguém para excluir hoje.")
        
    # MUDANÇA: Retorna os DataFrames inteiros, não só True/False
    return df_incluir, df_excluir

# --- ETAPAS DE UPLOAD (4 e 5) ---
def etapa_4_upload_inclusao():
    print("--- Iniciando Etapa 4: Upload Automático (Inclusão) ---")
    
    if not os.path.exists(config.ARQUIVO_INCLUSAO):
        print("Arquivo de inclusão não encontrado. Pulando etapa.")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        
        print("Acessando site...")
        page.goto("https://beneficiosplaces.gointegro.com/adminpanel/home")
        page.fill('input[type="text"]', config.EMAIL_PLACES)
        page.fill('input[type="password"]', config.SENHA_PLACES)
        page.click('button:has-text("Iniciar sessão")') 
        
        print("Navegando para Gerenciar Pessoas...")
        page.wait_for_selector('text="Pessoas"', timeout=15000)
        page.click('text="Pessoas"')
        page.wait_for_selector('text="Gerenciar"', timeout=15000)
        page.click('text="Gerenciar"')
        time.sleep(3)

        print("Abrindo modal de importação...")
        page.click('text="Importar com CSV"')
        
        print("Selecionando modo: ADICIONAR")
        page.click('button:has-text("Adicionar")') 
        time.sleep(1)

        print("Enviando arquivo de inclusão...")
        caminho_arquivo = os.path.abspath(config.ARQUIVO_INCLUSAO)
        page.locator('.addfile input[type="file"]').set_input_files(caminho_arquivo)
        
        print("Validando...")
        page.click('text="Validar"')
        
        try:
            print("Aguardando confirmação do site...")
            # Ajuste o texto do seletor final conforme o que aparecer no botão de sucesso
            page.wait_for_selector('text="Importação"', timeout=60000) 
            page.click('text="Importação"') # Ou "Ok"
            print("✅ Upload de INCLUSÃO concluído!")
        except Exception as e:
            print(f"⚠️ Erro na confirmação final: {e}")
        
        time.sleep(5)
        browser.close()

def etapa_5_upload_exclusao():
    print("--- Iniciando Etapa 5: Upload Automático (Exclusão/Churn) ---")
    
    if not os.path.exists(config.ARQUIVO_EXCLUSAO):
        print("Arquivo de exclusão não encontrado. Pulando etapa.")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        
        print("Acessando site...")
        page.goto("https://beneficiosplaces.gointegro.com/adminpanel/home")
        page.fill('input[type="text"]', config.EMAIL_PLACES)
        page.fill('input[type="password"]', config.SENHA_PLACES)
        page.click('button:has-text("Iniciar sessão")') 
        
        print("Navegando para Gerenciar Pessoas...")
        page.wait_for_selector('text="Pessoas"', timeout=15000)
        page.click('text="Pessoas"')
        page.wait_for_selector('text="Gerenciar"', timeout=15000)
        page.click('text="Gerenciar"')
        time.sleep(3)

        print("Abrindo modal de importação...")
        page.click('text="Importar com CSV"')
        
        # --- A MUDANÇA PRINCIPAL ESTÁ AQUI ---
        print("Selecionando modo: ATUALIZAR")
        page.click('button:has-text("Atualizar")') 
        time.sleep(1)
        # -------------------------------------

        print("Enviando arquivo de exclusão...")
        caminho_arquivo = os.path.abspath(config.ARQUIVO_EXCLUSAO)
        page.locator('.addfile input[type="file"]').set_input_files(caminho_arquivo)
        
        print("Validando...")
        page.click('text="Validar"')
        
        try:
            print("Aguardando confirmação do site...")
            page.wait_for_selector('text="Atualizar"', timeout=60000) 
            page.click('text="Atualizar"')
            print("✅ Upload de EXCLUSÃO concluído!")
        except Exception as e:
            print(f"⚠️ Erro na confirmação final: {e}")
        
        time.sleep(5)
        browser.close()
        
# --- GERAR LOG HISTÓRICO ---
def salvar_historico(df_inc, df_exc):
    print("--- Salvando Log Histórico ---")
    arquivo_log = "historico_geral.csv"
    data_hora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    logs = []
    
    # Prepara log de inclusão
    if not df_inc.empty:
        # Cria uma cópia para não alterar o arquivo original de upload
        temp_inc = df_inc.copy()
        temp_inc['Data_Execucao'] = data_hora
        temp_inc['Tipo_Acao'] = 'INCLUSAO'
        # Seleciona colunas relevantes
        temp_inc = temp_inc[['Data_Execucao', 'Tipo_Acao', 'first_name', 'country', 'groups']]
        logs.append(temp_inc)
        
    # Prepara log de exclusão
    if not df_exc.empty:
        temp_exc = df_exc.copy()
        temp_exc['Data_Execucao'] = data_hora
        temp_exc['Tipo_Acao'] = 'EXCLUSAO'
        temp_exc = temp_exc[['Data_Execucao', 'Tipo_Acao', 'first_name', 'country', 'groups']]
        logs.append(temp_exc)
    
    # Se tiver algo para salvar
    if logs:
        df_final_log = pd.concat(logs)
        
        # Modo 'a' (append) adiciona ao final sem apagar o arquivo
        # header=not os.path.exists... só escreve o cabeçalho se o arquivo não existir
        escrever_cabecalho = not os.path.exists(arquivo_log)
        df_final_log.to_csv(arquivo_log, mode='a', header=escrever_cabecalho, index=False)
        print(f"📝 Histórico atualizado em: {arquivo_log}")
    else:
        print("📝 Nada para registrar no histórico hoje.")

# --- EXECUTOR PRINCIPAL ---
if __name__ == "__main__":
    try:
        # 1. Baixar Places
        df_places = etapa_1_baixar_base_places()
        
        # 2. Baixar Carteira Meli
        df_bq = etapa_2_buscar_bigquery()
        
        # 3. Processar Regras (Agora retorna os DataFrames)
        df_incluir, df_excluir = etapa_3_processamento(df_bq, df_places)
        
        # 4. Salvar Log Histórico (NOVO!)
        salvar_historico(df_incluir, df_excluir)

        # Menu Interativo
        print("\n" + "="*40)
        print("      RESUMO DO PROCESSAMENTO")
        print("="*40)
        print(f"Inclusões pendentes: {len(df_incluir)}")
        print(f"Exclusões pendentes: {len(df_excluir)}")
        print("="*40 + "\n")

        if not df_incluir.empty:
            resp = input("Deseja fazer o upload automático da INCLUSÃO agora? (s/n): ")
            if resp.lower() == 's':
                etapa_4_upload_inclusao()
        
        if not df_excluir.empty:
            resp = input("Deseja fazer o upload automático da EXCLUSÃO agora? (s/n): ")
            if resp.lower() == 's':
                etapa_5_upload_exclusao()

        print("\n🚀 Processo Finalizado! Log salvo em 'historico_geral.csv'.")
        
    except Exception as e:
        print(f"\n❌ Erro fatal na execução: {e}")