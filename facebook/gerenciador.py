import streamlit as st
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta, date, timezone 
import time
import psycopg2 # Importa o driver PostgreSQL
from psycopg2 import Error as PgError # Erro específico do psycopg2
import traceback
import sqlite3
import pytz

# Importações da API do Facebook (mantidas)
try:
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.adaccount import AdAccount
    from facebook_business.adobjects.campaign import Campaign
    from facebook_business.adobjects.adset import AdSet
    from facebook_business.adobjects.ad import Ad
except ImportError:
    st.error("Biblioteca 'facebook_business' não encontrada. Instale com: pip install facebook-business")
    st.stop()

# CSS (mantido)
st.markdown("""
<style>
    /* Seu CSS aqui... (igual ao anterior) */
    .main .block-container { padding-top: 2rem; padding-left: 2rem; padding-right: 2rem; }
    .stTabs [data-baseweb="tab-list"] { gap: 2rem; }
    div[data-testid="stVerticalBlock"] > div[data-testid="stForm"] > div[data-testid="stVerticalBlock"],
    div[data-testid="stVerticalBlock"] > div[style*="border: 1px solid"] {
         border: 1px solid #ddd; padding: 1rem; border-radius: 0.5rem;
         background-color: #fafafa; margin-bottom: 1rem;
     }
    .success-badge { background-color: #28a745; color: white; padding: 3px 8px; border-radius: 5px; font-size: 0.85em; display: inline-block; text-align: center; min-width: 60px;}
    .error-badge { background-color: #dc3545; color: white; padding: 3px 8px; border-radius: 5px; font-size: 0.85em; display: inline-block; text-align: center; min-width: 60px;}
    .warning-badge { background-color: #ffc107; color: black; padding: 3px 8px; border-radius: 5px; font-size: 0.85em; display: inline-block; text-align: center; min-width: 60px;}
    .info-badge { background-color: #17a2b8; color: white; padding: 3px 8px; border-radius: 5px; font-size: 0.85em; display: inline-block; text-align: center; min-width: 60px;}
    .inactive-badge { background-color: #6c757d; color: white; padding: 3px 8px; border-radius: 5px; font-size: 0.85em; display: inline-block; text-align: center; min-width: 60px;}
</style>
""", unsafe_allow_html=True)

# --- Funções do Banco de Dados (ADAPTADAS PARA POSTGRESQL) ---

# Cache para conexão (evita reconectar a cada interação mínima, mas reconecta se der erro)
@st.cache_resource(ttl=3600)
def get_db_connection():
    """Obtém uma conexão com o banco de dados."""
    # Verificar variáveis de ambiente para PostgreSQL
    pg_host = os.getenv("PGHOST")
    pg_user = os.getenv("PGUSER")
    pg_password = os.getenv("PGPASSWORD")
    
    # Usar SQLite como fallback se não houver configuração PostgreSQL
    if not (pg_host and pg_user and pg_password):
        try:
            import sqlite3
            # Criar pasta data se não existir
            if not os.path.exists("data"):
                os.makedirs("data")
            # Retornar SQLite com uma tupla indicando o tipo
            return (sqlite3.connect("data/gcoperacional.db", check_same_thread=False), "sqlite")
        except Exception as e:
            st.error(f"Erro ao conectar ao SQLite: {e}")
            return None
    
    # Usar PostgreSQL se houver configuração
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=pg_host,
            port=os.getenv("PGPORT"),
            database=os.getenv("PGDATABASE"),
            user=pg_user,
            password=pg_password
        )
        return (conn, "postgres")
    except Exception as e:
        st.error(f"Erro ao conectar ao PostgreSQL: {e}")
        return None

def close_connection(conn_info):
    """Fecha a conexão com o banco de dados."""
    if conn_info is None:
        return
    
    conn, conn_type = conn_info
    if conn is not None:
        try:
            conn.close()
        except Exception as e:
            print(f"Erro ao fechar conexão: {e}")

def execute_query(query, params=None, fetch_one=False, fetch_all=False, is_dml=False):
    """Executa uma query no banco de dados com melhor tratamento de erros."""
    conn_info = get_db_connection()
    if conn_info is None:
        return None
    
    conn, conn_type = conn_info
    result = None
    cursor = None
    
    try:
        # Verificar e reconectar se necessário
        if conn_type == "postgres" and conn.closed:
            get_db_connection.clear()
            conn_info = get_db_connection()
            if conn_info is None:
                return None
            conn, conn_type = conn_info
        
        cursor = conn.cursor()
        
        # Adaptar placeholders conforme o tipo de banco
        adapted_query = query
        if params is not None:
            if conn_type == "sqlite" and "%s" in query:
                # Converter placeholders %s para ? no SQLite
                adapted_query = query.replace("%s", "?")
            elif conn_type == "postgres" and "?" in query:
                # Converter placeholders ? para %s no PostgreSQL
                adapted_query = query.replace("?", "%s")
            
            cursor.execute(adapted_query, params)
        else:
            cursor.execute(adapted_query)
        
        if is_dml:  # Data Manipulation Language (INSERT, UPDATE, DELETE)
            conn.commit()
            result = cursor.rowcount
        elif fetch_one:
            result = cursor.fetchone()
        elif fetch_all:
            result = cursor.fetchall()
        
        if not is_dml:
            conn.commit()
    
    except Exception as e:
        try:
            conn.rollback()
        except:
            pass
        print(f"Erro na query: {e}\nQuery: {query}\nParams: {params}")
        st.error(f"Erro na operação do banco de dados")
        result = None
    
    finally:
        if cursor:
            cursor.close()
    
    return result

def init_db():
    """Inicializa o banco de dados, criando/atualizando tabelas."""
    conn_info = get_db_connection()
    if conn_info is None:
        st.error("Não foi possível inicializar o banco de dados: Falha na conexão.")
        return

    conn, conn_type = conn_info
    cursor = None
    print(f"Inicializando/Verificando DB ({conn_type})...") # Log

    try:
        cursor = conn.cursor()

        # --- Tabela api_config (sem alterações aqui, mantenha como está no seu código original) ---
        if conn_type == "sqlite":
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS api_config (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, app_id TEXT NOT NULL,
                    app_secret TEXT NOT NULL, access_token TEXT NOT NULL, account_id TEXT NOT NULL,
                    business_id TEXT, page_id TEXT, is_active INTEGER DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP, token_expires_at DATE
                )
            ''')
            cursor.execute("PRAGMA table_info(api_config)")
            columns_api = [info[1] for info in cursor.fetchall()]
            if 'token_expires_at' not in columns_api:
                print("Adicionando coluna 'token_expires_at' em api_config (SQLite)...")
                cursor.execute('ALTER TABLE api_config ADD COLUMN token_expires_at DATE')
                conn.commit() # Commit após alteração
        else: # PostgreSQL
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS api_config (
                    id SERIAL PRIMARY KEY, name TEXT NOT NULL, app_id TEXT NOT NULL,
                    app_secret TEXT NOT NULL, access_token TEXT NOT NULL, account_id TEXT NOT NULL,
                    business_id TEXT, page_id TEXT, is_active INTEGER DEFAULT 0,
                    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, token_expires_at DATE
                )
            ''')
            cursor.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'api_config' AND column_name = 'token_expires_at';
            """)
            column_exists_api = cursor.fetchone()
            if not column_exists_api:
                print("Adicionando coluna 'token_expires_at' em api_config (PostgreSQL)...")
                try:
                    cursor.execute('ALTER TABLE api_config ADD COLUMN token_expires_at DATE')
                    conn.commit() # Commit após alteração
                except psycopg2.Error as e_alter_api:
                    conn.rollback()
                    if "already exists" in str(e_alter_api).lower(): print("Coluna token_expires_at já existe (ignorado).")
                    else: print(f"Erro add coluna token_expires_at: {e_alter_api}")


        # --- Tabela rules (MODIFICADA PARA INCLUIR NOVOS CAMPOS) ---
        print("Verificando/Atualizando tabela 'rules'...")
        rules_columns_to_add = {
            "execution_mode": "TEXT DEFAULT 'manual'",
            "execution_interval_hours": "INTEGER",
            "last_automatic_run_at": "TIMESTAMP WITH TIME ZONE" if conn_type == "postgres" else "TIMESTAMP"
        }

        if conn_type == "sqlite":
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL, description TEXT, condition_type TEXT NOT NULL,
                    is_composite INTEGER DEFAULT 0, primary_metric TEXT NOT NULL,
                    primary_operator TEXT NOT NULL, primary_value REAL NOT NULL,
                    secondary_metric TEXT, secondary_operator TEXT, secondary_value REAL,
                    join_operator TEXT DEFAULT 'AND', action_type TEXT NOT NULL, action_value REAL,
                    is_active INTEGER DEFAULT 1,
                    execution_mode TEXT DEFAULT 'manual', -- NOVO
                    execution_interval_hours INTEGER, -- NOVO
                    last_automatic_run_at TIMESTAMP, -- NOVO
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute("PRAGMA table_info(rules)")
            existing_columns_rules = [info[1] for info in cursor.fetchall()]
            columns_added_sqlite = False
            for col_name, col_type_def in rules_columns_to_add.items():
                 sql_type = col_type_def # Usa a definição padrão
                 if col_name == 'last_automatic_run_at' and conn_type == 'sqlite': sql_type = 'TIMESTAMP'
                 if col_name == 'execution_mode' and conn_type == 'sqlite': sql_type = "TEXT DEFAULT 'manual'"

                 if col_name not in existing_columns_rules:
                     print(f"Adicionando coluna '{col_name}' à tabela 'rules' (SQLite)...")
                     cursor.execute(f'ALTER TABLE rules ADD COLUMN {col_name} {sql_type}')
                     columns_added_sqlite = True
            if columns_added_sqlite: conn.commit() # Commit se adicionou colunas

        else: # PostgreSQL
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS rules (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL, description TEXT, condition_type TEXT NOT NULL,
                    is_composite INTEGER DEFAULT 0, primary_metric TEXT NOT NULL,
                    primary_operator TEXT NOT NULL, primary_value REAL NOT NULL,
                    secondary_metric TEXT, secondary_operator TEXT, secondary_value REAL,
                    join_operator TEXT DEFAULT 'AND', action_type TEXT NOT NULL, action_value REAL,
                    is_active INTEGER DEFAULT 1,
                    execution_mode TEXT DEFAULT 'manual', -- NOVO
                    execution_interval_hours INTEGER, -- NOVO
                    last_automatic_run_at TIMESTAMP WITH TIME ZONE, -- NOVO (com timezone)
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'rules';
            """)
            existing_columns_rules = [row[0] for row in cursor.fetchall()]
            for col_name, col_type_def in rules_columns_to_add.items():
                 if col_name not in existing_columns_rules:
                     print(f"Adicionando coluna '{col_name}' à tabela 'rules' (PostgreSQL)...")
                     try:
                         cursor.execute(f'ALTER TABLE rules ADD COLUMN {col_name} {col_type_def}')
                         conn.commit() # Commit após cada ALTER TABLE para segurança
                     except psycopg2.Error as e_alter_rules:
                         conn.rollback()
                         if "already exists" in str(e_alter_rules).lower(): print(f"Coluna '{col_name}' já existe (ignorado).")
                         else: print(f"Erro add coluna {col_name}: {e_alter_rules}")


        # --- Tabela rule_executions (sem alterações aqui, mantenha como está no seu código original) ---
        if conn_type == "sqlite":
             cursor.execute('''
                CREATE TABLE IF NOT EXISTS rule_executions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, rule_id INTEGER NOT NULL, ad_object_id TEXT NOT NULL,
                    ad_object_type TEXT NOT NULL, ad_object_name TEXT NOT NULL,
                    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, was_successful INTEGER DEFAULT 0, message TEXT,
                    FOREIGN KEY (rule_id) REFERENCES rules (id) ON DELETE CASCADE
                )
             ''')
        else: # PostgreSQL
             cursor.execute('''
                CREATE TABLE IF NOT EXISTS rule_executions (
                    id SERIAL PRIMARY KEY, rule_id INTEGER NOT NULL, ad_object_id TEXT NOT NULL,
                    ad_object_type TEXT NOT NULL, ad_object_name TEXT NOT NULL,
                    executed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, was_successful INTEGER DEFAULT 0, message TEXT,
                    FOREIGN KEY (rule_id) REFERENCES rules (id) ON DELETE CASCADE
                )
             ''')

        # Não precisa de commit final aqui, pois fizemos commit após cada ALTER TABLE

        print("Verificação/Atualização do schema do DB concluída.")

    except (psycopg2.Error, sqlite3.Error, Exception) as e: # Adiciona sqlite3.Error
        error_type = type(e).__name__
        print(f"Erro Crítico durante init_db ({error_type}): {e}\n{traceback.format_exc()}")
        st.error(f"Erro CRÍTICO ao inicializar/atualizar o banco de dados: {e}")
        try:
            if conn: conn.rollback() # Tenta reverter
            print("Rollback realizado devido a erro na inicialização.")
        except Exception as rb_err:
            print(f"Erro durante o rollback na inicialização: {rb_err}")

    finally:
        if cursor:
            cursor.close()

init_db()

def save_api_config(name, app_id, app_secret, access_token, account_id, business_id="", page_id="", token_expires_at=None):
    """Salva a configuração da API no banco de dados."""
    # Verifica quantas configs existem para definir is_active
    conn_info = get_db_connection()
    if conn_info is None:
        return False
    
    conn, conn_type = conn_info
    cursor = None
    
    try:
        cursor = conn.cursor()
        # Contar configs existentes
        cursor.execute("SELECT COUNT(*) FROM api_config")
        count_result = cursor.fetchone()
        count = count_result[0] if count_result else 0
        is_active = 1 if count == 0 else 0
        
        # Formata a data para objeto date ou None
        expires_at_date = token_expires_at if isinstance(token_expires_at, date) else None
        
        # Ajustar query conforme tipo de banco
        if conn_type == "sqlite":
            insert_query = """
                INSERT INTO api_config
                (name, app_id, app_secret, access_token, account_id,
                 business_id, page_id, is_active, token_expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        else:  # PostgreSQL
            insert_query = """
                INSERT INTO api_config
                (name, app_id, app_secret, access_token, account_id,
                 business_id, page_id, is_active, token_expires_at, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """
        
        params = (
            name, app_id, app_secret, access_token, account_id,
            business_id if business_id else None,
            page_id if page_id else None,
            is_active,
            expires_at_date
        )
        
        cursor.execute(insert_query, params)
        conn.commit()
        return True
        
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
        print(f"Erro ao salvar config: {e}")
        return False
        
    finally:
        if cursor:
            cursor.close()

@st.cache_data(ttl=60)
def get_active_api_config():
    """Obtém a configuração ativa do PostgreSQL."""
    query = """
        SELECT id, name, app_id, app_secret, access_token, account_id,
               business_id, page_id, token_expires_at -- Coluna DATE
        FROM api_config WHERE is_active = 1 LIMIT 1
    """
    row = execute_query(query, fetch_one=True)
    if row:
        # Mapeia colunas para dicionário
        keys = ["id", "name", "app_id", "app_secret", "access_token", "account_id",
                "business_id", "page_id", "token_expires_at"]
        return dict(zip(keys, row))
    return None

@st.cache_data(ttl=60)
def get_all_api_configs():
    """Obtém todas as configurações do PostgreSQL."""
    query = """
        SELECT id, name, app_id, app_secret, access_token, account_id,
               business_id, page_id, is_active, token_expires_at
        FROM api_config ORDER BY name
    """
    rows = execute_query(query, fetch_all=True)
    configs = []
    if rows:
        keys = ["id", "name", "app_id", "app_secret", "access_token", "account_id",
                "business_id", "page_id", "is_active", "token_expires_at"]
        for row in rows:
            configs.append(dict(zip(keys, row)))
    return configs

def set_active_api_config(config_id):
    """
    Define a configuração de API especificada como ativa no banco de dados.
    (Versão Otimizada: sem limpar cache de conexão e sem st.rerun explícito)
    """
    print(f"DEBUG: set_active_api_config chamada para ID: {config_id}") # Log para depuração
    conn_info = get_db_connection()
    if conn_info is None:
        st.error("Falha ao obter conexão com o banco de dados para definir config ativa.")
        return False

    conn, conn_type = conn_info
    success = False
    cursor = None

    try:
        # Verificar se a conexão está aberta (especialmente para PG)
        if conn_type == "postgres":
            # Verifica se conn.closed não é 0 (está fechada)
            connection_closed = getattr(conn, 'closed', 0) != 0
            if connection_closed:
                st.warning("Conexão PG estava fechada ao tentar definir config ativa. Tentando reconectar...")
                # Limpa APENAS o cache da conexão se ela ESTAVA fechada
                get_db_connection.clear()
                conn_info = get_db_connection()
                if conn_info is None:
                     st.error("Falha ao reconectar ao banco de dados.")
                     return False
                conn, conn_type = conn_info
                # Não precisa criar cursor aqui ainda

        # Criar cursor agora que temos certeza que a conexão está (ou deveria estar) aberta
        cursor = conn.cursor()

        print(f"DEBUG: Desativando todas as configs...")
        # Desativa todas as outras configs
        if conn_type == "sqlite":
            sql_deactivate = "UPDATE api_config SET is_active = 0"
            cursor.execute(sql_deactivate)
        else: # PostgreSQL
            sql_deactivate = "UPDATE api_config SET is_active = 0"
            cursor.execute(sql_deactivate)
        print(f"DEBUG: {cursor.rowcount} configs desativadas.")

        print(f"DEBUG: Ativando config ID {config_id}...")
        # Ativa a config selecionada
        if conn_type == "sqlite":
            sql_activate = "UPDATE api_config SET is_active = 1 WHERE id = ?"
            params_activate = (config_id,)
            cursor.execute(sql_activate, params_activate)
        else: # PostgreSQL
            sql_activate = "UPDATE api_config SET is_active = 1 WHERE id = %s"
            params_activate = (config_id,)
            cursor.execute(sql_activate, params_activate)

        conn.commit() # Confirma as alterações (desativar e ativar)
        # Verifica se o UPDATE de ativação afetou alguma linha
        success = cursor.rowcount > 0
        print(f"DEBUG: Config ID {config_id} ativada? {'Sim' if success else 'Não'}. Linhas afetadas: {cursor.rowcount}")

    except (PgError, sqlite3.Error, Exception) as e: # Inclui sqlite3.Error
        error_type = type(e).__name__
        st.error(f"Erro ({error_type}) ao definir configuração ativa: {e}")
        print(f"Erro detalhado em set_active_api_config: {traceback.format_exc()}")
        if conn:
            try:
                conn.rollback() # Tenta reverter em caso de erro
                print("Rollback da transação realizado.")
            except Exception as rb_err:
                print(f"Erro durante o rollback da transação: {rb_err}")
        success = False

    finally:
        if cursor:
            try:
                cursor.close()
            except Exception as cur_close_err:
                print(f"Erro ao fechar o cursor: {cur_close_err}")
        # NÃO fechamos a conexão principal aqui, ela é gerenciada pelo @st.cache_resource

    # A limpeza de cache e o st.rerun devem acontecer DEPOIS que esta função
    # for chamada com sucesso, lá na parte do selectbox da interface.

    return success


def delete_api_config(config_id):
    """Exclui uma configuração do banco de dados."""
    conn_info = get_db_connection()
    if conn_info is None: 
        return False
    
    conn, conn_type = conn_info
    success = False
    cursor = None
    
    try:
        cursor = conn.cursor()
        
        # Verifica se a que será excluída é a ativa
        if conn_type == "sqlite":
            cursor.execute("SELECT is_active FROM api_config WHERE id = ?", (config_id,))
        else:
            cursor.execute("SELECT is_active FROM api_config WHERE id = %s", (config_id,))
            
        row = cursor.fetchone()
        is_active_to_delete = row and row[0] == 1

        # Exclui a configuração
        if conn_type == "sqlite":
            cursor.execute("DELETE FROM api_config WHERE id = ?", (config_id,))
        else:
            cursor.execute("DELETE FROM api_config WHERE id = %s", (config_id,))
            
        deleted_count = cursor.rowcount

        # Se excluiu a ativa, tenta ativar outra
        if deleted_count > 0 and is_active_to_delete:
            cursor.execute("SELECT id FROM api_config ORDER BY id LIMIT 1")
            other_config = cursor.fetchone()
            if other_config:
                if conn_type == "sqlite":
                    cursor.execute("UPDATE api_config SET is_active = 1 WHERE id = ?", (other_config[0],))
                else:
                    cursor.execute("UPDATE api_config SET is_active = 1 WHERE id = %s", (other_config[0],))

        conn.commit()
        success = deleted_count > 0
        
    except Exception as e:
        try:
            conn.rollback()
        except:
            pass
        st.error(f"Erro ao excluir configuração: {e}")
        success = False
        
    finally:
        if cursor:
            cursor.close()

    return success


# --- Funções da API do Facebook (Adaptadas para checar token_expires_at tipo date) ---
def init_facebook_api():
    """Inicializa a API do Facebook com as credenciais ativas do PostgreSQL."""
    config = get_active_api_config()
    if config:
        if not all([config.get("app_id"), config.get("app_secret"), config.get("access_token"), config.get("account_id")]):
             st.error("Configuração ativa está incompleta (faltam App ID, Secret, Token ou Account ID). Verifique na aba 'Configurações'.")
             return None

        # Verifica se o token expirou (agora compara objetos date)
        expires_date = config.get('token_expires_at') # Já vem como objeto date ou None
        if isinstance(expires_date, date):
            if expires_date < date.today():
                st.error(f"O Token de Acesso para a conta '{config.get('name')}' expirou em {expires_date.strftime('%d/%m/%Y')}. Atualize-o na aba 'Configurações'.")
                return None # Impede inicialização com token expirado

        try:
            FacebookAdsApi.init(
                app_id=config["app_id"],
                app_secret=config["app_secret"],
                access_token=config["access_token"],
                api_version='v22.0'
            )
            # Verifica a conexão
            try:
                AdAccount(f'act_{config["account_id"]}').api_get(fields=['id'])
                return config["account_id"]
            except Exception as conn_err:
                 st.error(f"Erro ao verificar conexão com a conta act_{config['account_id']}: {conn_err}. Verifique o Token de Acesso e o Account ID.")
                 return None
        except Exception as e:
            st.error(f"Erro CRÍTICO ao inicializar API do Facebook: {e}")
            return None
    else:
        return None

# --- Funções de Insights e Campanhas (get_campaign_insights_cached, get_facebook_campaigns_cached) ---
# NENHUMA ALTERAÇÃO necessária aqui, pois elas dependem de init_facebook_api que já foi adaptada.
# Cole as funções get_campaign_insights_cached e get_facebook_campaigns_cached do seu código anterior aqui.
@st.cache_data(ttl=300) # Cache por 5 minutos
def get_campaign_insights_cached(account_id, campaign_ids_tuple, time_range='last_7d'):
    """Busca insights para uma lista de campanhas (cacheado)."""
    campaign_ids = list(campaign_ids_tuple) # Converte tuple de volta para lista
    if not account_id or not campaign_ids:
        return []
    try:
        params = {
            'level': 'campaign',
            'filtering': [{'field': 'campaign.id', 'operator': 'IN', 'value': campaign_ids}],
            'breakdowns': []
        }
        # Define o período de tempo
        if time_range == 'yesterday':
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            params['time_range'] = {'since': yesterday, 'until': yesterday}
        elif time_range == 'last_7d': params['date_preset'] = 'last_7d'
        elif time_range == 'last_30d': params['date_preset'] = 'last_30d'
        else: params['date_preset'] = 'last_7d' # Default

        account = AdAccount(f'act_{account_id}')
        insights = account.get_insights(
            params=params,
            fields=[
                'campaign_id', 'campaign_name', 'spend', 'impressions', 'clicks',
                'ctr', 'cpc', 'actions', 'cost_per_action_type', 'purchase_roas'
            ]
        )
        processed_insights = []
        for insight in insights:
            insight_dict = insight.export_all_data()
            purchases = 0
            purchase_value = 0.0
            # Extrai ações de compra e valor
            if 'actions' in insight_dict:
                for action in insight_dict['actions']:
                    if action.get('action_type') == 'purchase':
                        purchases = int(action.get('value', 0))
                    # Tenta pegar valor de compra (pode variar nome da action)
                    if action.get('action_type') in ['offsite_conversion.fb_pixel_purchase', 'purchase', 'omni_purchase']:
                         action_values = action.get('action_values')
                         if isinstance(action_values, list) and len(action_values) > 0:
                             purchase_value += float(action_values[0].get('value', 0.0))
                         else:
                             purchase_value += float(action.get('value', 0.0))


            # Extrai CPA de compra
            cpa = 0.0
            if 'cost_per_action_type' in insight_dict:
                for cost_action in insight_dict['cost_per_action_type']:
                    if cost_action.get('action_type') == 'purchase':
                        cpa = float(cost_action.get('value', 0.0))
                        break

            # Extrai ROAS
            roas = 0.0
            if 'purchase_roas' in insight_dict:
                 roas_list = insight_dict['purchase_roas']
                 if roas_list and isinstance(roas_list, list) and len(roas_list) > 0:
                     roas = float(roas_list[0].get('value', 0.0))

            insight_dict['purchases'] = purchases
            insight_dict['cpa'] = cpa
            insight_dict['roas'] = roas
            insight_dict['purchase_value'] = purchase_value

            processed_insights.append(insight_dict)
        return processed_insights
    except Exception as e:
        st.error(f"Erro ao obter insights de campanhas (ID: {', '.join(campaign_ids)}): {e}")
        return []

@st.cache_data(ttl=300) # Cache por 5 minutos
def get_facebook_campaigns_cached(account_id_from_main):
    """Busca todas as campanhas e seus insights recentes (cacheado)."""
    campaigns_result = []
    try:
        initialized_account_id = init_facebook_api()
        if not initialized_account_id:
            return None

        if account_id_from_main != initialized_account_id:
             st.warning(f"Inconsistência de Account ID: Esperado {account_id_from_main}, API inicializada com {initialized_account_id}. Usando {initialized_account_id}.")
             target_account_id = initialized_account_id
        else:
             target_account_id = initialized_account_id

        account = AdAccount(f'act_{target_account_id}')
        fields_to_fetch = [
            'id', 'name', 'status', 'objective', 'created_time',
            'start_time', 'stop_time', 'daily_budget', 'lifetime_budget',
            'effective_status', 'buying_type', 'budget_remaining'
        ]
        campaigns_raw = list(account.get_campaigns(fields=fields_to_fetch, params={'limit': 500}))

        if not campaigns_raw:
             return []

        campaign_ids = [campaign.get("id") for campaign in campaigns_raw if campaign.get("id")]
        if not campaign_ids: return []

        insights_data = get_campaign_insights_cached(target_account_id, tuple(campaign_ids), "last_7d")
        if insights_data is None: insights_data = []

        insights_map = {insight.get("campaign_id"): insight for insight in insights_data if insight.get("campaign_id")}

        for campaign in campaigns_raw:
            campaign_dict = campaign.export_all_data()
            campaign_id = campaign_dict.get("id")
            if not campaign_id: continue

            campaign_insights = insights_map.get(campaign_id)
            if campaign_insights:
                campaign_dict["insights"] = {
                    "cpa": float(campaign_insights.get("cpa", 0.0)),
                    "purchases": int(campaign_insights.get("purchases", 0)),
                    "roas": float(campaign_insights.get("roas", 0.0)),
                    "purchase_value": float(campaign_insights.get("purchase_value", 0.0)),
                    "spend": float(campaign_insights.get("spend", 0.0)),
                    "clicks": int(campaign_insights.get("clicks", 0)),
                    "impressions": int(campaign_insights.get("impressions", 0)),
                    "ctr": float(campaign_insights.get("ctr", 0.0)),
                    "cpc": float(campaign_insights.get("cpc", 0.0)),
                }
            else:
                campaign_dict["insights"] = {
                    "cpa": 0.0, "purchases": 0, "roas": 0.0, "purchase_value": 0.0,
                    "spend": 0.0, "clicks": 0, "impressions": 0, "ctr": 0.0, "cpc": 0.0
                }

            daily_budget_str = campaign_dict.get('daily_budget')
            lifetime_budget_str = campaign_dict.get('lifetime_budget')
            campaign_dict['daily_budget'] = int(daily_budget_str) if daily_budget_str and daily_budget_str.isdigit() else 0
            campaign_dict['lifetime_budget'] = int(lifetime_budget_str) if lifetime_budget_str and lifetime_budget_str.isdigit() else 0

            campaigns_result.append(campaign_dict)

        return campaigns_result

    except PgError as db_err: # Captura erro do PostgreSQL também
         st.error(f"Erro de banco de dados em get_facebook_campaigns: {db_err}")
         return None
    except Exception as e:
        st.error(f"Erro CRÍTICO inesperado em get_facebook_campaigns: {e}")
        return None


# --- Funções de Regras (Adaptadas para PostgreSQL) ---
def add_rule(name, description, primary_metric, primary_operator,
             primary_value, action_type, action_value, is_composite=0, secondary_metric=None,
             secondary_operator=None, secondary_value=None, join_operator="AND",
             execution_mode='manual', execution_interval_hours=None): # Novos parâmetros com default
    """Adiciona uma nova regra ao banco de dados."""

    # --- DEBUG PRINT ---
    print(f"DEBUG [add_rule]: Função recebendo Modo='{execution_mode}', Intervalo={execution_interval_hours}")
    # --- FIM DEBUG PRINT ---

    conn_info = get_db_connection()
    if conn_info is None:
        st.error("Falha ao obter conexão com DB para adicionar regra.") # Mensagem para UI
        return False

    conn, conn_type = conn_info
    cursor = None
    success = False

    try:
        cursor = conn.cursor()

        # Limpa o intervalo se o modo for manual, garantindo consistência
        if execution_mode == 'manual':
            execution_interval_hours = None
        # Garante que intervalo seja um número ou None
        elif execution_interval_hours is not None:
            try:
                execution_interval_hours = int(execution_interval_hours)
            except (ValueError, TypeError):
                print(f"AVISO [add_rule]: Intervalo inválido recebido ({execution_interval_hours}), definindo como None.")
                execution_interval_hours = None # Define como None se não for um inteiro válido

        # Query adaptada para incluir novos campos
        if conn_type == "sqlite":
            query = """
                INSERT INTO rules
                (name, description, condition_type, is_composite, primary_metric,
                 primary_operator, primary_value, secondary_metric, secondary_operator,
                 secondary_value, join_operator, action_type, action_value,
                 execution_mode, execution_interval_hours, -- Novos campos
                 updated_at, created_at) -- Adicionado created_at para consistência
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now')) -- 16 placeholders
            """
        else: # PostgreSQL
            query = """
                INSERT INTO rules
                (name, description, condition_type, is_composite, primary_metric,
                 primary_operator, primary_value, secondary_metric, secondary_operator,
                 secondary_value, join_operator, action_type, action_value,
                 execution_mode, execution_interval_hours, -- Novos campos
                 updated_at, created_at) -- Adicionado created_at para consistência
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) -- 16 placeholders
            """

        params = (
            name, description, "custom", is_composite, primary_metric,
            primary_operator, primary_value, secondary_metric, secondary_operator,
            secondary_value, join_operator, action_type, action_value,
            execution_mode, # Novo
            execution_interval_hours # Novo
        )

        # Usar execute_query para consistência no tratamento de erros e placeholders
        rowcount = execute_query(query, params, is_dml=True)

        # execute_query adaptada retorna rowcount (>=0) ou None em caso de erro
        if rowcount is not None and rowcount > 0:
             success = True
             get_all_rules_cached.clear() # Limpa cache para UI atualizar
             print(f"Regra '{name}' adicionada com modo '{execution_mode}'.")
        elif rowcount == 0:
             print(f"AVISO [add_rule]: Insert da regra '{name}' não afetou linhas.")
             # Pode não ser um erro fatal, mas é estranho.
        else: # rowcount is None (erro ocorreu dentro de execute_query)
             success = False
             # O erro já foi logado/mostrado por execute_query

    except Exception as e: # Captura erro geral se execute_query falhar antes
        print(f"Erro GERAL ao adicionar regra: {e}\n{traceback.format_exc()}")
        st.error(f"Erro inesperado ao adicionar regra: {e}") # Para UI
        success = False

    # Não precisa fechar cursor ou conexão aqui, execute_query (e get_db_connection) cuidam disso

    return success

@st.cache_data(ttl=60)
def get_all_rules_cached():
    """Busca todas as regras do banco de dados (cacheado), incluindo modo de execução."""
    print("DEBUG: Executando get_all_rules_cached...") # Log para ver se está sendo chamada
    conn_info = get_db_connection()
    if conn_info is None:
        st.error("Falha ao obter conexão com DB para buscar regras.")
        return []

    conn, conn_type = conn_info
    rules_list = []
    cursor = None

    try:
        cursor = conn.cursor()

        # Query SQL - *** CORRIGIDA PARA INCLUIR AS NOVAS COLUNAS ***
        query = """
            SELECT
                id, name, description, condition_type, is_composite,
                primary_metric, primary_operator, primary_value,
                secondary_metric, secondary_operator, secondary_value,
                join_operator, action_type, action_value, is_active,
                execution_mode, execution_interval_hours, last_automatic_run_at, -- <<< NOVAS COLUNAS AQUI
                created_at, updated_at
            FROM rules
            ORDER BY created_at DESC
        """

        cursor.execute(query)
        rows = cursor.fetchall()
        print(f"DEBUG: get_all_rules_cached encontrou {len(rows)} linhas.") # Log

        if rows:
            # Lista de Nomes de Colunas - *** CORRIGIDA PARA INCLUIR AS NOVAS COLUNAS ***
            # A ORDEM DEVE SER EXATAMENTE A MESMA DO SELECT ACIMA!
            columns = [
                "id", "name", "description", "condition_type", "is_composite",
                "primary_metric", "primary_operator", "primary_value",
                "secondary_metric", "secondary_operator", "secondary_value",
                "join_operator", "action_type", "action_value", "is_active",
                "execution_mode", "execution_interval_hours", "last_automatic_run_at", # <<< NOVAS COLUNAS AQUI
                "created_at", "updated_at"
            ]

            for row in rows:
                # Cria o dicionário mapeando nome da coluna para valor da linha
                rule_dict = dict(zip(columns, row))
                # Debug: Imprime o dicionário para ver se os novos campos estão lá
                # print(f"DEBUG: Rule Dict: {rule_dict}")
                rules_list.append(rule_dict)

    except (PgError, sqlite3.Error, Exception) as e: # Captura erros
        error_type = type(e).__name__
        print(f"Erro ao buscar regras ({error_type}): {e}")
        st.error(f"Erro ao buscar regras do banco de dados: {e}") # Mostra erro na UI

    finally:
        if cursor:
            cursor.close()
        # Não fecha a conexão principal aqui (gerenciada pelo cache)

    print(f"DEBUG: get_all_rules_cached retornando {len(rules_list)} regras.") # Log
    return rules_list

@st.cache_data(ttl=60)
def get_rule_executions_cached(limit=20):
    """Busca as últimas execuções de regras (cacheado)."""
    conn_info = get_db_connection()
    if conn_info is None:
        return []
    
    conn, conn_type = conn_info
    executions_list = []
    cursor = None
    
    try:
        cursor = conn.cursor()
        
        if conn_type == "sqlite":
            query = """
                SELECT re.id, r.name as rule_name, re.rule_id, re.ad_object_id, re.ad_object_type,
                       re.ad_object_name, re.executed_at, re.was_successful, re.message
                FROM rule_executions re
                LEFT JOIN rules r ON re.rule_id = r.id
                ORDER BY re.executed_at DESC
                LIMIT ?
            """
            cursor.execute(query, (limit,))
        else:
            query = """
                SELECT re.id, r.name as rule_name, re.rule_id, re.ad_object_id, re.ad_object_type,
                       re.ad_object_name, re.executed_at, re.was_successful, re.message
                FROM rule_executions re
                LEFT JOIN rules r ON re.rule_id = r.id
                ORDER BY re.executed_at DESC
                LIMIT %s
            """
            cursor.execute(query, (limit,))
            
        rows = cursor.fetchall()
        
        if rows:
            columns = ["id", "rule_name", "rule_id", "ad_object_id", "ad_object_type",
                       "ad_object_name", "executed_at", "was_successful", "message"]
            for row in rows:
                execution_dict = dict(zip(columns, row))
                if execution_dict.get("rule_name") is None and execution_dict.get("rule_id"):
                    execution_dict["rule_name"] = f"Regra ID {execution_dict['rule_id']} (Excluída)"
                elif execution_dict.get("rule_name") is None:
                    execution_dict["rule_name"] = "Regra Desconhecida"
                executions_list.append(execution_dict)
                
    except Exception as e:
        print(f"Erro ao buscar execuções: {e}")
        
    finally:
        if cursor:
            cursor.close()
            
    return executions_list

def delete_rule(rule_id):
    """Exclui uma regra do banco de dados."""
    conn_info = get_db_connection()
    if conn_info is None:
        return False
    
    conn, conn_type = conn_info
    cursor = None
    success = False
    
    try:
        cursor = conn.cursor()
        
        # Adapta a sintaxe conforme o tipo de banco
        if conn_type == "sqlite":
            query = "DELETE FROM rules WHERE id = ?"
        else:
            query = "DELETE FROM rules WHERE id = %s"
            
        params = (rule_id,)
        cursor.execute(query, params)
        conn.commit()
        
        # Verifica se alguma linha foi afetada
        success = cursor.rowcount > 0
        
        if success:
            get_all_rules_cached.clear()  # Limpa cache
        
    except Exception as e:
        try:
            conn.rollback()
        except:
            pass
        print(f"Erro ao excluir regra ID {rule_id}: {e}")
        st.error(f"Erro ao excluir regra: {str(e)[:100]}")
        success = False
        
    finally:
        if cursor:
            cursor.close()
            
    return success


def toggle_rule_status(rule_id, is_active):
    """Ativa ou desativa uma regra no banco de dados."""
    conn_info = get_db_connection()
    if conn_info is None:
        return False
    
    conn, conn_type = conn_info
    cursor = None
    success = False
    
    try:
        cursor = conn.cursor()
        
        if conn_type == "sqlite":
            query = "UPDATE rules SET is_active = ?, updated_at = datetime('now') WHERE id = ?"
            params = (1 if is_active else 0, rule_id)
        else:
            query = "UPDATE rules SET is_active = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s"
            params = (1 if is_active else 0, rule_id)
            
        cursor.execute(query, params)
        conn.commit()
        get_all_rules_cached.clear()
        success = cursor.rowcount > 0
        
    except Exception as e:
        try:
            conn.rollback()
        except:
            pass
        print(f"Erro ao alterar status da regra: {e}")
        success = False
        
    finally:
        if cursor:
            cursor.close()
            
    return success

def log_rule_execution(rule_id, ad_object_id, ad_object_type, ad_object_name, was_successful, message=""):
    """Registra a execução de uma regra no banco de dados."""
    conn_info = get_db_connection()
    if conn_info is None:
        print(f"Falha ao registrar execução: conexão nula")
        return False
    
    conn, conn_type = conn_info
    cursor = None
    success = False
    
    try:
        cursor = conn.cursor()
        
        # Query adaptada por tipo de banco
        if conn_type == "sqlite":
            query = """
                INSERT INTO rule_executions
                (rule_id, ad_object_id, ad_object_type, ad_object_name, was_successful, message)
                VALUES (?, ?, ?, ?, ?, ?)
            """
        else:
            query = """
                INSERT INTO rule_executions
                (rule_id, ad_object_id, ad_object_type, ad_object_name, was_successful, message)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            
        params = (rule_id, ad_object_id, ad_object_type, ad_object_name, 1 if was_successful else 0, message)
        cursor.execute(query, params)
        conn.commit()
        
        # Limpa cache após inserção bem-sucedida
        get_rule_executions_cached.clear()
        success = True
        
    except Exception as e:
        try:
            conn.rollback()
        except:
            pass
        print(f"Erro ao registrar execução no DB: {e}")
        success = False
        
    finally:
        if cursor:
            cursor.close()
            
    return success


@st.cache_data(ttl=60)
def get_rule_executions_cached(limit=20):
    """Busca as últimas execuções de regras (cacheado)."""
    conn_info = get_db_connection()
    if conn_info is None:
        return []
    
    conn, conn_type = conn_info
    executions_list = []
    cursor = None
    
    try:
        cursor = conn.cursor()
        
        # Query adaptada por tipo de banco
        if conn_type == "sqlite":
            query = """
                SELECT re.id, r.name as rule_name, re.rule_id, re.ad_object_id, re.ad_object_type,
                       re.ad_object_name, re.executed_at, re.was_successful, re.message
                FROM rule_executions re
                LEFT JOIN rules r ON re.rule_id = r.id
                ORDER BY re.executed_at DESC
                LIMIT ?
            """
            cursor.execute(query, (limit,))
        else:
            query = """
                SELECT re.id, r.name as rule_name, re.rule_id, re.ad_object_id, re.ad_object_type,
                       re.ad_object_name, re.executed_at, re.was_successful, re.message
                FROM rule_executions re
                LEFT JOIN rules r ON re.rule_id = r.id
                ORDER BY re.executed_at DESC
                LIMIT %s
            """
            cursor.execute(query, (limit,))
            
        rows = cursor.fetchall()
        
        if rows:
            columns = ["id", "rule_name", "rule_id", "ad_object_id", "ad_object_type",
                       "ad_object_name", "executed_at", "was_successful", "message"]
            for row in rows:
                execution_dict = dict(zip(columns, row))
                if execution_dict.get("rule_name") is None and execution_dict.get("rule_id"):
                    execution_dict["rule_name"] = f"Regra ID {execution_dict['rule_id']} (Excluída)"
                elif execution_dict.get("rule_name") is None:
                    execution_dict["rule_name"] = "Regra Desconhecida"
                executions_list.append(execution_dict)
                
    except Exception as e:
        print(f"Erro ao buscar execuções: {e}")
        # Não mostra erro na tela para evitar poluição visual
        
    finally:
        if cursor:
            cursor.close()
            
    return executions_list

def get_rule_executions_by_date(start_date, end_date):
    """
    Busca execuções de regras dentro de um intervalo de datas específico.

    Args:
        start_date (datetime.date): A data de início do intervalo.
        end_date (datetime.date): A data de fim do intervalo.

    Returns:
        list: Uma lista de dicionários representando as execuções encontradas,
              ou uma lista vazia se ocorrer um erro ou nada for encontrado.
    """
    conn_info = get_db_connection()
    if conn_info is None:
        st.error("Falha na conexão com o DB para buscar histórico por data.")
        return []

    conn, conn_type = conn_info
    executions_list = []
    cursor = None

    try:
        cursor = conn.cursor()

        # --- IMPORTANTE: Ajuste para incluir o dia final completo ---
        # Adiciona 1 dia ao end_date para a comparação ser "menor que o dia seguinte"
        # Isso garante que todos os registros do end_date sejam incluídos.
        end_date_adjusted = end_date + timedelta(days=1)

        # Query base (igual à da função cacheada, mas sem LIMIT)
        query_base = """
            SELECT re.id, r.name as rule_name, re.rule_id, re.ad_object_id, re.ad_object_type,
                   re.ad_object_name, re.executed_at, re.was_successful, re.message
            FROM rule_executions re
            LEFT JOIN rules r ON re.rule_id = r.id
        """

        # Adiciona a condição WHERE para filtrar por data
        # Atenção aos placeholders (%s ou ?)
        if conn_type == "sqlite":
            query = f"{query_base} WHERE re.executed_at >= ? AND re.executed_at < ? ORDER BY re.executed_at DESC"
            params = (start_date, end_date_adjusted)
        else: # PostgreSQL
            # Assume que executed_at é TIMESTAMP WITH TIME ZONE ou similar
            query = f"{query_base} WHERE re.executed_at >= %s AND re.executed_at < %s ORDER BY re.executed_at DESC"
            params = (start_date, end_date_adjusted)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        if rows:
            columns = ["id", "rule_name", "rule_id", "ad_object_id", "ad_object_type",
                       "ad_object_name", "executed_at", "was_successful", "message"]
            for row in rows:
                execution_dict = dict(zip(columns, row))
                # Lógica para nome da regra (se excluída)
                if execution_dict.get("rule_name") is None and execution_dict.get("rule_id"):
                    execution_dict["rule_name"] = f"Regra ID {execution_dict['rule_id']} (Excluída)"
                elif execution_dict.get("rule_name") is None:
                    execution_dict["rule_name"] = "Regra Desconhecida"
                executions_list.append(execution_dict)

    except (PgError, Exception) as e:
        st.error(f"Erro ao buscar histórico por data: {e}")
        print(f"Erro detalhado em get_rule_executions_by_date: {traceback.format_exc()}")
        executions_list = [] # Retorna lista vazia em caso de erro

    finally:
        if cursor:
            cursor.close()

    return executions_list


# --- Funções de Execução e Simulação de Regras ---
# NENHUMA ALTERAÇÃO necessária aqui, pois elas dependem das funções de DB/API já adaptadas.
# Cole as funções execute_rule e simulate_rule_application do seu código anterior aqui.
def execute_rule(campaign_id, rule_id):
    """Executa a ação definida por uma regra em uma campanha específica."""
    get_facebook_campaigns_cached.clear()
    get_campaign_insights_cached.clear()
    get_rule_executions_cached.clear()

    campaign_name = f'Campanha ID {campaign_id}'
    try:
        account_id = init_facebook_api()
        if not account_id:
            return False, "Não foi possível inicializar a API do Facebook"

        rules = get_all_rules_cached()
        rule = next((r for r in rules if r["id"] == rule_id), None)
        if not rule:
            log_rule_execution(rule_id, campaign_id, 'campaign', campaign_name, False, "Regra não encontrada no banco de dados")
            return False, "Regra não encontrada"
        if not rule.get('is_active'):
            return False, "Regra está inativa"

        campaign_obj = Campaign(campaign_id)
        campaign_data = campaign_obj.api_get(fields=['name', 'status', 'daily_budget', 'lifetime_budget'])
        campaign_name = campaign_data.get('name', campaign_name)

        success = False
        message = ""
        action_params = {}

        current_daily_budget = int(campaign_data.get('daily_budget', 0))
        current_lifetime_budget = int(campaign_data.get('lifetime_budget', 0))

        action_type = rule['action_type']
        action_value = rule.get('action_value')

        if action_type == 'duplicate_budget':
            if current_daily_budget > 0:
                new_budget = current_daily_budget * 2
                action_params = {'daily_budget': new_budget}
                message = f"Orçamento diário duplicado de {current_daily_budget/100:.2f} para {new_budget/100:.2f}"
            elif current_lifetime_budget > 0:
                new_budget = current_lifetime_budget * 2
                action_params = {'lifetime_budget': new_budget}
                message = f"Orçamento total duplicado de {current_lifetime_budget/100:.2f} para {new_budget/100:.2f}"
            else: message = "Nenhum orçamento encontrado para duplicar"; success = False; action_params=None

        elif action_type == 'triple_budget':
            if current_daily_budget > 0:
                new_budget = current_daily_budget * 3
                action_params = {'daily_budget': new_budget}
                message = f"Orçamento diário triplicado de {current_daily_budget/100:.2f} para {new_budget/100:.2f}"
            elif current_lifetime_budget > 0:
                new_budget = current_lifetime_budget * 3
                action_params = {'lifetime_budget': new_budget}
                message = f"Orçamento total triplicado de {current_lifetime_budget/100:.2f} para {new_budget/100:.2f}"
            else: message = "Nenhum orçamento encontrado para triplicar"; success = False; action_params=None

        elif action_type == 'pause_campaign':
            if campaign_data.get('status') == Campaign.Status.active:
                action_params = {'status': Campaign.Status.paused}
                message = "Campanha pausada"
            else: message = "Campanha já não estava ativa"; success = True; action_params=None

        elif action_type == 'activate_campaign':
             if campaign_data.get('status') == Campaign.Status.paused:
                action_params = {'status': Campaign.Status.active}
                message = "Campanha ativada"
             else: message = "Campanha já não estava pausada"; success = True; action_params=None

        elif action_type == 'halve_budget':
            min_budget_cents = 100
            if current_daily_budget > 0:
                new_budget = max(min_budget_cents, current_daily_budget // 2)
                action_params = {'daily_budget': new_budget}
                message = f"Orçamento diário reduzido para {new_budget/100:.2f} (era {current_daily_budget/100:.2f})"
            elif current_lifetime_budget > 0:
                new_budget = max(min_budget_cents, current_lifetime_budget // 2)
                action_params = {'lifetime_budget': new_budget}
                message = f"Orçamento total reduzido para {new_budget/100:.2f} (era {current_lifetime_budget/100:.2f})"
            else: message = "Nenhum orçamento encontrado para reduzir"; success = False; action_params=None

        elif action_type == 'custom_budget_multiplier' and action_value is not None:
            multiplier = float(action_value)
            min_budget_cents = 100
            if current_daily_budget > 0:
                new_budget = max(min_budget_cents, int(current_daily_budget * multiplier))
                action_params = {'daily_budget': new_budget}
                message = f"Orçamento diário multiplicado por {multiplier:.2f} para {new_budget/100:.2f} (era {current_daily_budget/100:.2f})"
            elif current_lifetime_budget > 0:
                new_budget = max(min_budget_cents, int(current_lifetime_budget * multiplier))
                action_params = {'lifetime_budget': new_budget}
                message = f"Orçamento total multiplicado por {multiplier:.2f} para {new_budget/100:.2f} (era {current_lifetime_budget/100:.2f})"
            else: message = "Nenhum orçamento encontrado para multiplicar"; success = False; action_params=None
        elif action_type == 'custom_budget_multiplier' and action_value is None:
             message = "Multiplicador de orçamento personalizado não definido na regra"; success = False; action_params=None
        else:
             message = f"Tipo de ação desconhecido ou inválido: {action_type}"; success = False; action_params=None

        if action_params is not None:
            try:
                campaign_obj.api_update(params=action_params)
                success = True
            except Exception as api_err:
                message = f"Erro da API ao aplicar ação '{action_type}': {api_err}"
                success = False

        log_rule_execution(
            rule_id=rule_id, ad_object_id=campaign_id, ad_object_type='campaign',
            ad_object_name=campaign_name, was_successful=success, message=message
        )
        return success, message

    except Exception as e:
        error_message = f"Erro inesperado ao aplicar regra ID {rule_id} na campanha {campaign_id}: {str(e)}"
        log_rule_execution(
            rule_id=rule_id, ad_object_id=campaign_id, ad_object_type='campaign',
            ad_object_name=campaign_name, was_successful=False, message=error_message
        )
        return False, error_message

def simulate_rule_application(campaign, rules):
    """Verifica quais regras ativas teriam suas condições atendidas para uma campanha."""
    rule_results = []
    if not campaign or not isinstance(campaign, dict) or "insights" not in campaign:
        return []

    metrics = {
        'cpa': campaign["insights"].get("cpa", 0.0),
        'purchases': campaign["insights"].get("purchases", 0),
        'roas': campaign["insights"].get("roas", 0.0),
        'spend': campaign["insights"].get("spend", 0.0),
        'clicks': campaign["insights"].get("clicks", 0),
        'ctr': campaign["insights"].get("ctr", 0.0),
        'cpc': campaign["insights"].get("cpc", 0.0),
    }
    current_daily_budget = campaign.get('daily_budget', 0)
    current_lifetime_budget = campaign.get('lifetime_budget', 0)
    current_budget = current_daily_budget if current_daily_budget > 0 else current_lifetime_budget

    for rule in rules:
        if not rule.get('is_active', 1) or not isinstance(rule, dict): continue

        primary_metric_key = rule.get('primary_metric')
        primary_operator = rule.get('primary_operator')
        primary_value_rule = rule.get('primary_value')

        if None in [primary_metric_key, primary_operator] or primary_value_rule is None or primary_metric_key not in metrics:
            continue

        primary_value_campaign = metrics[primary_metric_key]
        primary_condition_met = False
        try:
            rule_val_p = float(primary_value_rule)
            camp_val_p = float(primary_value_campaign)

            if primary_operator == '<' and camp_val_p < rule_val_p: primary_condition_met = True
            elif primary_operator == '<=' and camp_val_p <= rule_val_p: primary_condition_met = True
            elif primary_operator == '>' and camp_val_p > rule_val_p: primary_condition_met = True
            elif primary_operator == '>=' and camp_val_p >= rule_val_p: primary_condition_met = True
            elif primary_operator == '==' and camp_val_p == rule_val_p: primary_condition_met = True
        except (TypeError, ValueError):
            continue

        condition_met = primary_condition_met

        if rule.get('is_composite', 0):
            secondary_metric_key = rule.get('secondary_metric')
            secondary_operator = rule.get('secondary_operator')
            secondary_value_rule = rule.get('secondary_value')
            join_operator = rule.get('join_operator', 'AND')

            if None in [secondary_metric_key, secondary_operator] or secondary_value_rule is None or secondary_metric_key not in metrics:
                 if join_operator == 'AND': condition_met = False
            else:
                secondary_value_campaign = metrics[secondary_metric_key]
                secondary_condition_met = False
                try:
                    rule_val_s = float(secondary_value_rule)
                    camp_val_s = float(secondary_value_campaign)

                    if secondary_operator == '<' and camp_val_s < rule_val_s: secondary_condition_met = True
                    elif secondary_operator == '<=' and camp_val_s <= rule_val_s: secondary_condition_met = True
                    elif secondary_operator == '>' and camp_val_s > rule_val_s: secondary_condition_met = True
                    elif secondary_operator == '>=' and camp_val_s >= rule_val_s: secondary_condition_met = True
                    elif secondary_operator == '==' and camp_val_s == rule_val_s: secondary_condition_met = True
                except (TypeError, ValueError):
                    secondary_condition_met = False

                if join_operator == 'AND': condition_met = primary_condition_met and secondary_condition_met
                elif join_operator == 'OR': condition_met = primary_condition_met or secondary_condition_met
                else: condition_met = False

        if condition_met:
            action_type = rule['action_type']
            action_value = rule.get('action_value')
            action_text = ""
            new_budget_simulated = None
            min_budget_cents = 100

            if action_type == 'duplicate_budget':
                action_text = "Duplicar orçamento"
                if current_budget > 0: new_budget_simulated = current_budget * 2
            elif action_type == 'triple_budget':
                action_text = "Triplicar orçamento"
                if current_budget > 0: new_budget_simulated = current_budget * 3
            elif action_type == 'pause_campaign':
                action_text = "Pausar campanha"
            elif action_type == 'activate_campaign':
                action_text = "Ativar campanha"
            elif action_type == 'halve_budget':
                action_text = "Reduzir orçamento pela metade"
                if current_budget > 0: new_budget_simulated = max(min_budget_cents, current_budget // 2)
            elif action_type == 'custom_budget_multiplier' and action_value is not None:
                try:
                    multiplier = float(action_value)
                    action_text = f"Multiplicar orçamento por {multiplier:.2f}"
                    if current_budget > 0: new_budget_simulated = max(min_budget_cents, int(current_budget * multiplier))
                except ValueError:
                     action_text = "Multiplicar orçamento (valor inválido!)"
            elif action_type == 'custom_budget_multiplier' and action_value is None:
                 action_text = "Multiplicar orçamento (valor não definido!)"
            else:
                 action_text = f"Ação desconhecida ({action_type})"

            rule_results.append({
                "rule_id": rule['id'],
                "rule_name": rule.get('name', 'Regra sem nome'),
                "action": action_text,
                "new_budget_simulated": new_budget_simulated
            })

    return rule_results


# --- Funções de UI (show_rule_form, format_rule_text) ---
# NENHUMA ALTERAÇÃO necessária aqui.
# Cole as funções show_rule_form e format_rule_text do seu código anterior aqui.
def show_rule_form():
    """Exibe o formulário para criar/editar uma regra."""
    METRIC_OPTIONS = {
        "cpa": "CPA (Custo por Aquisição)", "purchases": "Compras", "roas": "ROAS",
        "spend": "Gasto (7d)", "clicks": "Cliques (7d)", "ctr": "CTR (%)", "cpc": "CPC"
    }
    OPERATOR_OPTIONS = { "<": "<", "<=": "<=", ">": ">", ">=": ">=", "==": "==" }
    ACTION_OPTIONS = {
        "duplicate_budget": "Duplicar orçamento", "triple_budget": "Triplicar orçamento",
        "pause_campaign": "Pausar campanha", "activate_campaign": "Ativar campanha",
        "halve_budget": "Reduzir orçamento pela metade",
        "custom_budget_multiplier": "Multiplicar orçamento por valor"
    }
    INTERVAL_OPTIONS = {
        1: "A cada 1 Hora", 3: "A cada 3 Horas", 6: "A cada 6 Horas",
        12: "A cada 12 Horas", 24: "A cada 24 Horas"
    }
    INTERVAL_OPTIONS_DISPLAY = INTERVAL_OPTIONS

    # --- INICIALIZAÇÃO DO SESSION STATE ---
    st.session_state.setdefault('rule_form_is_composite', False)
    st.session_state.setdefault('rule_form_primary_metric', 'cpa')
    st.session_state.setdefault('rule_form_secondary_metric', 'purchases')
    st.session_state.setdefault('rule_form_action_type', 'pause_campaign')
    st.session_state.setdefault('rule_form_execution_mode', 'manual')
    st.session_state.setdefault('rule_form_interval', 6)
    st.session_state.setdefault('rule_form_join_operator', 'AND')
    st.session_state.setdefault('rule_form_primary_operator', '<=')
    st.session_state.setdefault('rule_form_secondary_operator', '>=')
    st.session_state.setdefault("rule_form_action_value", 1.2)

    current_pm_init = st.session_state.get('rule_form_primary_metric', 'cpa')
    current_sm_init = st.session_state.get('rule_form_secondary_metric', 'purchases')
    is_composite_init = st.session_state.get('rule_form_is_composite', False)
    is_float_p_init = current_pm_init in ['cpa', 'roas', 'cpc', 'ctr', 'spend']
    st.session_state.setdefault(f"rule_form_primary_value_{current_pm_init}", 0.0 if is_float_p_init else 0)
    if is_composite_init:
        is_float_s_init = current_sm_init in ['cpa', 'roas', 'cpc', 'ctr', 'spend']
        st.session_state.setdefault(f"rule_form_secondary_value_{current_sm_init}", 0.0 if is_float_s_init else 0)
    # --- FIM DA INICIALIZAÇÃO ---

    # --- CONTROLES EXTERNOS AO FORMULÁRIO ---
    st.markdown("##### Configuração Geral e Modo")
    st.checkbox("Usar duas condições (regra composta)", key='rule_form_is_composite')
    is_composite_state = st.session_state['rule_form_is_composite']
    st.radio(
        "Modo de Execução:", options=['manual', 'automatic'],
        format_func=lambda x: {'manual': 'Manual', 'automatic': 'Automático'}.get(x),
        key='rule_form_execution_mode', horizontal=True
    )
    current_mode_state = st.session_state['rule_form_execution_mode']
    if current_mode_state == 'automatic':
        interval_key = 'rule_form_interval'
        options_list_interval = list(INTERVAL_OPTIONS.keys())
        try:
             current_interval_value_state = st.session_state.get(interval_key, 6)
             default_index_interval = options_list_interval.index(current_interval_value_state)
        except (ValueError, IndexError, KeyError): default_index_interval = 2
        st.selectbox(
            "Executar a cada:", options=options_list_interval,
            format_func=lambda x: INTERVAL_OPTIONS[x], key=interval_key,
            index=default_index_interval, help="Com que frequência o sistema deve verificar?"
        )
    st.markdown("---")
    st.markdown("##### Seleção de Métricas")
    col_m1, col_m2 = st.columns(2)
    with col_m1:
        st.selectbox(
            "**1ª Métrica**", options=list(METRIC_OPTIONS.keys()),
            format_func=lambda x: METRIC_OPTIONS[x], key='rule_form_primary_metric'
        )
    with col_m2:
        if is_composite_state:
            st.selectbox(
                "**2ª Métrica**", options=list(METRIC_OPTIONS.keys()),
                format_func=lambda x: METRIC_OPTIONS[x], key='rule_form_secondary_metric'
            )
        else: st.markdown("")
    # --- FIM DOS CONTROLES EXTERNOS ---

    st.markdown("---")

    current_primary_metric = st.session_state['rule_form_primary_metric']
    current_secondary_metric = st.session_state.get('rule_form_secondary_metric', 'purchases')
    is_float_primary = current_primary_metric in ['cpa', 'roas', 'cpc', 'ctr', 'spend']
    is_float_secondary = False
    if is_composite_state:
        is_float_secondary = current_secondary_metric in ['cpa', 'roas', 'cpc', 'ctr', 'spend']

    # --- INÍCIO DO FORMULÁRIO ---
    with st.form("new_rule_form"):
        st.markdown("##### Detalhes, Condições e Ação")
        name = st.text_input("Nome da Regra*", key="rule_form_name")
        # --- CORREÇÃO APLICADA AQUI ---
        description = st.text_area("Descrição (Opcional)", key="rule_form_description") # Removido height=60
        # --- FIM DA CORREÇÃO ---

        if is_composite_state:
            st.radio(
                "Operador de Junção:", ["AND", "OR"], key="rule_form_join_operator",
                horizontal=True, format_func=lambda x: {"AND": "E (ambas)", "OR": "OU (uma ou ambas)"}.get(x)
            )
        st.markdown("---")
        st.markdown(f"**1ª Condição:** {METRIC_OPTIONS[current_primary_metric]}")
        col1_p_form, col2_p_form = st.columns([1, 2])
        with col1_p_form:
            primary_operator = st.selectbox("Operador", options=list(OPERATOR_OPTIONS.keys()),
                                            format_func=lambda x: OPERATOR_OPTIONS[x],
                                            key="rule_form_primary_operator")
        with col2_p_form:
            label_p = "Valor (R$)*" if is_float_primary else "Quantidade*"
            step_p = 0.01 if is_float_primary else 1
            format_p = "%.2f" if is_float_primary else "%d"
            primary_value_key = f"rule_form_primary_value_{current_primary_metric}"
            min_val_p = 0.0 if is_float_primary else 0
            if current_primary_metric == 'roas': min_val_p = None
            primary_value = st.number_input(label_p, min_value=min_val_p, step=step_p, format=format_p, key=primary_value_key)

        secondary_operator = None
        secondary_value = None
        if is_composite_state:
            st.markdown(f"**2ª Condição:** {METRIC_OPTIONS[current_secondary_metric]}")
            col1_s_form, col2_s_form = st.columns([1, 2])
            with col1_s_form:
                secondary_operator = st.selectbox("Operador", options=list(OPERATOR_OPTIONS.keys()),
                                                 format_func=lambda x: OPERATOR_OPTIONS[x],
                                                 key="rule_form_secondary_operator")
            with col2_s_form:
                label_s = "Valor (R$)*" if is_float_secondary else "Quantidade*"
                step_s = 0.01 if is_float_secondary else 1
                format_s = "%.2f" if is_float_secondary else "%d"
                secondary_value_key = f"rule_form_secondary_value_{current_secondary_metric}"
                min_val_s = 0.0 if is_float_secondary else 0
                if current_secondary_metric == 'roas': min_val_s = None
                secondary_value = st.number_input(label_s, min_value=min_val_s, step=step_s, format=format_s, key=secondary_value_key)

        st.markdown("---")
        st.markdown("##### Ação")
        col1_a_form, col2_a_form = st.columns(2)
        with col1_a_form:
             action_type_widget = st.selectbox(
                "Tipo de Ação*", options=list(ACTION_OPTIONS.keys()),
                format_func=lambda x: ACTION_OPTIONS[x],
                key='rule_form_action_type'
                )
        with col2_a_form:
            action_value_widget = None
            if action_type_widget == "custom_budget_multiplier":
                action_value_widget = st.number_input(
                    "Multiplicador*", min_value=0.1, step=0.1, format="%.2f",
                    key="rule_form_action_value", help="Ex: 1.2 para aumentar 20%"
                    )

        st.markdown("---")
        st.markdown("##### Resumo da Regra (Pré-visualização)")
        try:
            resumo_primary_metric = st.session_state['rule_form_primary_metric']
            resumo_secondary_metric = st.session_state.get('rule_form_secondary_metric', 'purchases')
            resumo_is_composite = st.session_state['rule_form_is_composite']
            resumo_execution_mode = st.session_state['rule_form_execution_mode']
            resumo_interval_hours = st.session_state.get('rule_form_interval')
            resumo_primary_operator = primary_operator
            resumo_secondary_operator = secondary_operator
            resumo_join_operator = st.session_state.get('rule_form_join_operator', 'AND')
            resumo_action_type = action_type_widget
            resumo_primary_value = primary_value
            resumo_secondary_value = secondary_value
            resumo_action_value = action_value_widget

            resumo_is_float_p = resumo_primary_metric in ['cpa', 'roas', 'cpc', 'ctr', 'spend']
            pv_resumo = resumo_primary_value if resumo_primary_value is not None else (0.0 if resumo_is_float_p else 0)
            val1_fmt_resumo = f"{float(pv_resumo):.2f}" if resumo_is_float_p else str(int(pv_resumo))
            rule_summary = f"**SE** {METRIC_OPTIONS.get(resumo_primary_metric, '?')} {OPERATOR_OPTIONS.get(resumo_primary_operator, '?')} {val1_fmt_resumo} "
            if resumo_is_composite:
                 resumo_is_float_s = resumo_secondary_metric in ['cpa', 'roas', 'cpc', 'ctr', 'spend']
                 sv_resumo = resumo_secondary_value if resumo_secondary_value is not None else (0.0 if resumo_is_float_s else 0)
                 val2_fmt_resumo = f"{float(sv_resumo):.2f}" if resumo_is_float_s else str(int(sv_resumo))
                 rule_summary += f"**{resumo_join_operator}** {METRIC_OPTIONS.get(resumo_secondary_metric, '?')} {OPERATOR_OPTIONS.get(resumo_secondary_operator, '?')} {val2_fmt_resumo} "
            action_text_summary = ACTION_OPTIONS.get(resumo_action_type, '?')
            if resumo_action_type == "custom_budget_multiplier":
                 av_resumo = resumo_action_value if resumo_action_value is not None else 1.2
                 action_text_summary += f" ({float(av_resumo):.2f})"
            rule_summary += f"**ENTÃO** {action_text_summary}"
            rule_summary += ".<br>**MODO:** "
            if resumo_execution_mode == 'automatic':
                if resumo_interval_hours:
                    interval_desc_resumo = INTERVAL_OPTIONS_DISPLAY.get(resumo_interval_hours, f"{resumo_interval_hours}h ?")
                    rule_summary += f"Automático ({interval_desc_resumo})"
                else: rule_summary += "Automático (Selecione Intervalo)"
            else: rule_summary += "Manual"
            st.markdown(rule_summary, unsafe_allow_html=True)
        except Exception as e: st.caption("Aguardando valores válidos para gerar resumo...")

        # --- Botão de Submit ---
        submitted = st.form_submit_button("💾 Criar Regra", use_container_width=True)
        if submitted:
            # --- Validação e Submissão ---
            final_name = st.session_state.get("rule_form_name", "")
            final_description = st.session_state.get("rule_form_description", "")
            final_execution_mode = st.session_state.get('rule_form_execution_mode', 'manual')
            final_interval_hours = st.session_state.get('rule_form_interval') if final_execution_mode == 'automatic' else None
            final_action_type = st.session_state.get('rule_form_action_type', 'pause_campaign')
            final_is_composite = st.session_state.get('rule_form_is_composite', False)
            final_primary_metric = st.session_state.get('rule_form_primary_metric', 'cpa')
            final_secondary_metric = st.session_state.get('rule_form_secondary_metric', 'purchases')
            final_primary_operator = st.session_state.get('rule_form_primary_operator', '<=')
            final_secondary_operator = st.session_state.get('rule_form_secondary_operator', '>=')
            final_join_operator = st.session_state.get('rule_form_join_operator', 'AND')
            final_pv_key = f"rule_form_primary_value_{final_primary_metric}"
            final_sv_key = f"rule_form_secondary_value_{final_secondary_metric}"
            final_av_key = "rule_form_action_value"
            submit_primary_value_final = st.session_state.get(final_pv_key, 0)
            submit_secondary_value_final = st.session_state.get(final_sv_key) if final_is_composite else None
            submit_action_value_final = st.session_state.get(final_av_key) if final_action_type == "custom_budget_multiplier" else None

            error = False
            if not final_name: st.error("O nome da regra é obrigatório."); error = True
            if submit_primary_value_final is None : st.error("O valor da 1ª condição é obrigatório."); error = True
            if final_is_composite and submit_secondary_value_final is None: st.error("O valor da 2ª condição é obrigatório."); error = True
            if final_action_type == "custom_budget_multiplier" and submit_action_value_final is None: st.error("O valor multiplicador é obrigatório."); error = True
            if final_execution_mode == 'automatic' and final_interval_hours is None: st.error("Selecione um intervalo para execução automática."); error = True

            if not error:
                pv_submit = float(submit_primary_value_final)
                sv_submit = float(submit_secondary_value_final) if final_is_composite and submit_secondary_value_final is not None else None
                av_submit = float(submit_action_value_final) if final_action_type == "custom_budget_multiplier" and submit_action_value_final is not None else None
                interval_submit = final_interval_hours

                print(f"DEBUG [Gerenciador]: Submit-> add_rule. Modo='{final_execution_mode}', Intervalo={interval_submit}, M1='{final_primary_metric}', V1={pv_submit}")
                add_rule_success = add_rule(
                    name=final_name, description=final_description, primary_metric=final_primary_metric,
                    primary_operator=final_primary_operator, primary_value=pv_submit, action_type=final_action_type,
                    action_value=av_submit, is_composite=1 if final_is_composite else 0,
                    secondary_metric=final_secondary_metric if final_is_composite else None,
                    secondary_operator=final_secondary_operator, secondary_value=sv_submit,
                    join_operator=final_join_operator, execution_mode=final_execution_mode,
                    execution_interval_hours=interval_submit
                )
                print(f"DEBUG [Gerenciador]: add_rule retornou: {add_rule_success}")

                if add_rule_success:
                    st.success("✅ Regra criada com sucesso!")
                    print("DEBUG [Gerenciador]: Definindo st.session_state.show_rule_form = False")
                    st.session_state.show_rule_form = False
                    keys_to_clear = [k for k in st.session_state if k.startswith('rule_form_')]
                    for key in keys_to_clear:
                        if key in st.session_state: del st.session_state[key]
                    print("DEBUG [Gerenciador]: Chamando st.rerun()")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("❌ Falha ao salvar a regra no banco de dados.")
                    print("DEBUG [Gerenciador]: add_rule falhou, formulário permanecerá aberto.")
            else: st.warning("Corrija os erros antes de salvar.")
                 
def format_rule_text(rule):
    """Formata a descrição de uma regra para exibição."""
    if not isinstance(rule, dict): return "Regra inválida"

    METRIC_OPTIONS = { "cpa": "CPA", "purchases": "Compras", "roas": "ROAS", "spend": "Gasto", "clicks": "Cliques", "ctr": "CTR", "cpc": "CPC" }
    OPERATOR_SYMBOLS = { "<": "<", "<=": "<=", ">": ">", ">=": ">=", "==": "==" }
    ACTION_TEXT = { "duplicate_budget": "duplicar orçamento", "triple_budget": "triplicar orçamento", "pause_campaign": "pausar campanha", "activate_campaign": "ativar campanha", "halve_budget": "reduzir orçamento pela metade", "custom_budget_multiplier": "multiplicar orçamento por" }

    try:
        rule_text = "SE "
        metric1 = rule.get('primary_metric')
        op1 = rule.get('primary_operator')
        val1 = rule.get('primary_value')
        if None not in [metric1, op1, val1]:
            is_float_m1 = metric1 in ['cpa', 'roas', 'cpc', 'ctr', 'spend']
            val1_fmt = f"{float(val1):.2f}" if is_float_m1 else str(int(float(val1)))
            rule_text += f"{METRIC_OPTIONS.get(metric1, '?')} {OPERATOR_SYMBOLS.get(op1, '?')} {val1_fmt}"
        else: rule_text += "[Condição 1 inválida]"

        if rule.get('is_composite'):
            join_op = rule.get('join_operator', 'AND')
            metric2 = rule.get('secondary_metric')
            op2 = rule.get('secondary_operator')
            val2 = rule.get('secondary_value')
            if None not in [metric2, op2, val2]:
                is_float_m2 = metric2 in ['cpa', 'roas', 'cpc', 'ctr', 'spend']
                val2_fmt = f"{float(val2):.2f}" if is_float_m2 else str(int(float(val2)))
                rule_text += f" **{join_op}** {METRIC_OPTIONS.get(metric2, '?')} {OPERATOR_SYMBOLS.get(op2, '?')} {val2_fmt}"
            else: rule_text += f" **{join_op}** [Condição 2 inválida]"

        action = rule.get('action_type')
        action_val = rule.get('action_value')
        rule_text += ", **ENTÃO** "
        if action:
            action_desc = ACTION_TEXT.get(action, '?')
            if action == 'custom_budget_multiplier':
                action_desc += f" {float(action_val):.2f}" if action_val is not None else " ?"
            rule_text += action_desc
        else: rule_text += "[Ação inválida]"

        return rule_text
    except Exception as e:
        print(f"Erro formatando regra {rule.get('id')}: {e}")
        return f"Erro ao formatar regra ID {rule.get('id')}"


# ==============================================================================
# Função Principal da Página (ADAPTADA PARA POSTGRESQL)
# ==============================================================================
def show_gerenciador_page():
    """Renderiza a página completa do Gerenciador de Anúncios."""
    # Obter configurações (ativas e todas)
    active_config = get_active_api_config()
    all_configs = get_all_api_configs()

    # Seletor de Conta (código igual ao anterior)
    col_empty_space, col_selector_widget = st.columns([3, 1])
    with col_selector_widget:
        if all_configs:
            config_options = {cfg['id']: f"{cfg['name']} ({cfg['account_id']})" for cfg in all_configs}
            active_config_id = active_config['id'] if active_config else None
            default_index = 0
            if active_config_id in config_options:
                try: default_index = list(config_options.keys()).index(active_config_id)
                except ValueError: default_index = 0

            selected_config_id = st.selectbox(
                "Conta:", options=list(config_options.keys()),
                format_func=lambda x: config_options.get(x, f"ID {x} Inválido"),
                index=default_index, label_visibility="collapsed", key="account_selector"
            )

            if selected_config_id != active_config_id:
                if set_active_api_config(selected_config_id):
                     st.toast(f"Conta '{config_options.get(selected_config_id, 'Selecionada')}' ativada!", icon="🔄")
                     # Limpeza de cache (incluindo caches relacionados a dados específicos da conta)
                     get_db_connection.clear() # Limpa cache da conexão
                     if 'get_facebook_campaigns_cached' in globals(): get_facebook_campaigns_cached.clear()
                     if 'get_campaign_insights_cached' in globals(): get_campaign_insights_cached.clear()
                     # Caches de regras geralmente não dependem da conta ativa, mas podem ser limpos por segurança
                     if 'get_all_rules_cached' in globals(): get_all_rules_cached.clear()
                     if 'get_rule_executions_cached' in globals(): get_rule_executions_cached.clear() # Limpa execuções para forçar recarga
                     time.sleep(0.5)
                     st.rerun()
                else:
                     st.error("Falha ao ativar a conta.")
                     # Não paramos aqui, mas o erro é mostrado

        elif not active_config and get_db_connection() is not None:
             st.info("Nenhuma conta configurada...")
        elif get_db_connection() is None:
             st.warning("⚠️ DB offline.")

    if not active_config and get_db_connection() is not None:
        st.warning("⚠️ Nenhuma conta do Facebook Ads está ativa. Vá para a aba '🔧 Configurações' para adicionar ou ativar uma conta.")

    # --- Interface principal com abas ---
    if get_db_connection() is not None:
        tabs = st.tabs(["📢 Campanhas", "⚙️ Regras", "🔧 Configurações"])

        # ==========================
        # Aba 1: Campanhas (e Histórico)
        # ==========================
        with tabs[0]:
            if not active_config:
                st.info("Selecione ou configure uma conta ativa na aba '🔧 Configurações' para ver os dados das campanhas.")
            else:
                # --- Obter dados das campanhas e regras ---
                data_placeholder = st.empty()
                data_placeholder.info(f"🔄 Carregando dados das campanhas da conta {active_config['account_id']}...")
                campaigns = get_facebook_campaigns_cached(active_config["account_id"]) # Assumindo que esta função existe e está correta
                rules = get_all_rules_cached() # Assumindo que esta função existe e está correta
                data_placeholder.empty()

                if campaigns is None:
                    st.warning("Não foi possível carregar os dados das campanhas. Verifique a aba 'Configurações' e a conexão.")
                elif not campaigns:
                    st.info(f"ℹ️ Nenhuma campanha encontrada para a conta ativa (act_{active_config['account_id']}).")
                else:
                    # --- Contagens e Filtro --- (Código igual ao anterior)
                    total_campaigns = len(campaigns)
                    active_statuses = ['ACTIVE']
                    active_campaigns_list = [c for c in campaigns if isinstance(c, dict) and c.get('effective_status', c.get('status')) in active_statuses]
                    active_campaigns_count = len(active_campaigns_list)
                    inactive_campaigns_count = total_campaigns - active_campaigns_count

                    filter_col, count_col = st.columns([3, 2])
                    with filter_col:
                        status_filter = st.radio("Filtrar por Status:", ["Todas", "Ativas", "Inativas"], index=0, horizontal=True, key="status_filter_radio")
                    with count_col:
                        # ... (código para exibir contagem) ...
                        if status_filter == "Ativas": count_text = f"<strong>{active_campaigns_count} Ativas</strong>"
                        elif status_filter == "Inativas": count_text = f"<strong>{inactive_campaigns_count} Inativas</strong>"
                        else: count_text = f"<strong>Total: {total_campaigns}</strong> ({active_campaigns_count} Ativas, {inactive_campaigns_count} Inativas)"
                        st.markdown(f"<div style='padding-top: 28px;'>{count_text}</div>", unsafe_allow_html=True)

                    st.markdown("<hr style='margin: 0.5rem 0;'>", unsafe_allow_html=True)

                    # --- Filtragem ---
                    filtered_campaigns = campaigns
                    if status_filter == "Ativas": filtered_campaigns = active_campaigns_list
                    elif status_filter == "Inativas": filtered_campaigns = [c for c in campaigns if c not in active_campaigns_list]

                    # --- Exibição das Campanhas ---
                    if not filtered_campaigns:
                        st.info(f"Nenhuma campanha encontrada com o status '{status_filter}'.")
                    else:
                        # Cabeçalho da tabela de campanhas
                        col_h = st.columns([3, 1.2, 0.8, 0.8, 0.8, 1.2, 2.5])
                        # ... (código dos cabeçalhos) ...
                        col_h[0].markdown("**Campanha**")
                        col_h[1].markdown("<div style='text-align: center;'><b>Status</b></div>", unsafe_allow_html=True)
                        col_h[2].markdown("<div style='text-align: center;'><b>CPA</b></div>", unsafe_allow_html=True)
                        col_h[3].markdown("<div style='text-align: center;'><b>Compras</b></div>", unsafe_allow_html=True)
                        col_h[4].markdown("<div style='text-align: center;'><b>ROAS</b></div>", unsafe_allow_html=True)
                        col_h[5].markdown("<div style='text-align: center;'><b>Ação Rápida</b></div>", unsafe_allow_html=True)
                        col_h[6].markdown("**Regras Aplicáveis**")
                        st.markdown("<hr style='margin: 0.1rem 0;'>", unsafe_allow_html=True)


                        # Loop para exibir cada campanha
                        for campaign in filtered_campaigns:
                            if not isinstance(campaign, dict): continue
                            campaign_id = campaign.get('id')
                            if not campaign_id: continue

                            cols = st.columns([3, 1.2, 0.8, 0.8, 0.8, 1.2, 2.5])

                            # Col 0: Nome, ID e Orçamento
                            # ... (código igual ao anterior) ...
                            cols[0].markdown(f"**{campaign.get('name', 'N/A')}**")
                            cols[0].caption(f"ID: `{campaign_id}`")
                            budget_text = ""
                            daily_budget_cents = campaign.get('daily_budget', 0)
                            lifetime_budget_cents = campaign.get('lifetime_budget', 0)
                            if daily_budget_cents > 0: budget_text = f"Diário: R$ {daily_budget_cents/100:.2f}"
                            elif lifetime_budget_cents > 0: budget_text = f"Total: R$ {lifetime_budget_cents/100:.2f}"
                            if budget_text: cols[0].caption(budget_text)


                            # Col 1: Status Efetivo
                            # ... (código igual ao anterior) ...
                            effective_status = campaign.get("effective_status", campaign.get("status", "UNKNOWN"))
                            status_map = {
                                'ACTIVE': ("success-badge", "ATIVO"), 'PAUSED': ("error-badge", "PAUSADO"),
                                'ARCHIVED': ("inactive-badge", "ARQUIVADO"), 'DELETED': ("inactive-badge", "DELETADO"),
                                # ... outros status
                                'UNKNOWN': ("inactive-badge", "DESCONHECIDO")
                            }
                            status_class, status_text = status_map.get(effective_status, ("inactive-badge", effective_status))
                            cols[1].markdown(f"<div style='text-align: center;'><span class='{status_class}'>{status_text}</span></div>", unsafe_allow_html=True)


                            # Col 2: CPA
                            # ... (código igual ao anterior) ...
                            cpa_value = campaign.get("insights", {}).get("cpa", 0.0)
                            cols[2].markdown(f"<div style='text-align: center;'>R$ {cpa_value:.2f}</div>", unsafe_allow_html=True)


                            # Col 3: Compras
                            # ... (código igual ao anterior) ...
                            purchases_value = campaign.get("insights", {}).get("purchases", 0)
                            cols[3].markdown(f"<div style='text-align: center;'>{purchases_value}</div>", unsafe_allow_html=True)


                            # Col 4: ROAS
                            # ... (código igual ao anterior) ...
                            roas_value = campaign.get("insights", {}).get("roas", 0.0)
                            cols[4].markdown(f"<div style='text-align: center;'>{roas_value:.2f}x</div>", unsafe_allow_html=True)


                            # Col 5: Ação Rápida (Pausar/Ativar Manualmente)
                            # ... (código igual ao anterior, com st.rerun e limpeza de cache nos botões) ...
                            with cols[5]:
                                action_button_placeholder = st.empty()
                                with action_button_placeholder:
                                    if effective_status == "ACTIVE":
                                        if st.button("⏸️ Pausar", key=f"pause_{campaign_id}", type="secondary", use_container_width=True, help="Pausar esta campanha"):
                                            try:
                                                # ... (lógica API para pausar) ...
                                                Campaign(campaign_id).api_update(params={'status': Campaign.Status.paused})
                                                st.success("Campanha pausada!")
                                                log_rule_execution(-1, campaign_id, 'campaign', campaign.get('name'), True, "Pausado manualmente via UI") # Assumindo que log_rule_execution existe
                                                # Limpeza de cache
                                                if 'get_facebook_campaigns_cached' in globals(): get_facebook_campaigns_cached.clear()
                                                if 'get_campaign_insights_cached' in globals(): get_campaign_insights_cached.clear()
                                                if 'get_rule_executions_cached' in globals(): get_rule_executions_cached.clear()
                                                time.sleep(1.5); st.rerun()
                                            except Exception as e:
                                                st.error(f"Erro ao pausar: {e}")
                                                log_rule_execution(-1, campaign_id, 'campaign', campaign.get('name'), False, f"Erro ao pausar via UI: {e}")
                                    elif effective_status == "PAUSED":
                                        if st.button("▶️ Ativar", key=f"activate_{campaign_id}", type="primary", use_container_width=True, help="Ativar esta campanha"):
                                            try:
                                                # ... (lógica API para ativar) ...
                                                Campaign(campaign_id).api_update(params={'status': Campaign.Status.active})
                                                st.success("Campanha ativada!")
                                                log_rule_execution(-2, campaign_id, 'campaign', campaign.get('name'), True, "Ativado manualmente via UI")
                                                # Limpeza de cache
                                                if 'get_facebook_campaigns_cached' in globals(): get_facebook_campaigns_cached.clear()
                                                if 'get_campaign_insights_cached' in globals(): get_campaign_insights_cached.clear()
                                                if 'get_rule_executions_cached' in globals(): get_rule_executions_cached.clear()
                                                time.sleep(1.5); st.rerun()
                                            except Exception as e:
                                                st.error(f"Erro ao ativar: {e}")
                                                log_rule_execution(-2, campaign_id, 'campaign', campaign.get('name'), False, f"Erro ao ativar via UI: {e}")
                                    else:
                                        st.caption("-") # Ou um placeholder

                            # Col 6: Regras Aplicáveis (Execução Manual de Regra)
                            # ... (código igual ao anterior, com st.rerun e limpeza de cache no botão "Aplicar") ...
                            with cols[6]:
                                rules_list_placeholder = st.empty()
                                with rules_list_placeholder.container():
                                    applicable_rules = []
                                    # ... (lógica can_simulate) ...
                                    can_simulate = (daily_budget_cents > 0 or lifetime_budget_cents > 0) and \
                                                   effective_status not in ['ARCHIVED', 'DELETED']
                                    if can_simulate:
                                        try:
                                            active_rules = [r for r in rules if r.get('is_active')]
                                            # Assumindo que simulate_rule_application existe
                                            applicable_rules = simulate_rule_application(campaign, active_rules)
                                        except Exception as sim_err: st.caption(f"Erro simulação: {sim_err}")
                                    # ... (lógica para exibir regras aplicáveis ou mensagens) ...

                                    if applicable_rules:
                                        for rule_sim in applicable_rules:
                                            # ... (código para exibir nome/ação da regra) ...
                                            if not isinstance(rule_sim, dict): continue
                                            rule_id_sim = rule_sim.get('rule_id')
                                            if not rule_id_sim: continue
                                            rule_cols = st.columns([4, 1.5])
                                            with rule_cols[0]:
                                                 # ... (exibe nome, ação, orçamento simulado) ...
                                                 rule_name_sim = rule_sim.get('rule_name', 'N/A')
                                                 rule_action_sim = rule_sim.get('action', 'N/A')
                                                 new_budget_sim = rule_sim.get('new_budget_simulated')
                                                 budget_sim_text = f" -> R$ {new_budget_sim/100:.2f}" if new_budget_sim is not None else ""
                                                 st.markdown(f"<small><i>{rule_name_sim} ({rule_action_sim}{budget_sim_text})</i></small>", unsafe_allow_html=True)

                                            with rule_cols[1]:
                                                can_apply = effective_status == 'ACTIVE' or 'Ativar campanha' in rule_action_sim
                                                if st.button("Aplicar", key=f"apply_{campaign_id}_{rule_id_sim}",
                                                            help=f"Executar regra '{rule_name_sim}' nesta campanha",
                                                            use_container_width=True, type="secondary", disabled=not can_apply):
                                                    st.info(f"Aplicando regra '{rule_name_sim}'...")
                                                    # Assumindo que execute_rule existe
                                                    success_exec, message_exec = execute_rule(campaign_id, rule_id_sim)
                                                    if success_exec:
                                                        st.success(f"✅ {message_exec}")
                                                        st.toast(f"Regra '{rule_name_sim}' aplicada!", icon="🎉")
                                                        # Limpeza de cache
                                                        if 'get_facebook_campaigns_cached' in globals(): get_facebook_campaigns_cached.clear()
                                                        if 'get_campaign_insights_cached' in globals(): get_campaign_insights_cached.clear()
                                                        if 'get_rule_executions_cached' in globals(): get_rule_executions_cached.clear()
                                                        time.sleep(2); st.rerun()
                                                    else:
                                                        st.error(f"❌ {message_exec}")
                                                        st.toast(f"Falha ao aplicar regra '{rule_name_sim}'!", icon="🔥")
                                    # ... (lógica else se nenhuma regra aplicável) ...
                                    elif can_simulate:
                                        st.caption("<div style='text-align: center; font-style: italic; font-size: 0.8em;'>Nenhuma regra ativa aplicável</div>", unsafe_allow_html=True)


                            # Linha divisória entre campanhas
                            st.markdown("<hr style='margin: 0.3rem 0;'>", unsafe_allow_html=True)


                # --- Histórico de Execuções (COM CONVERSÃO DE TIMEZONE) ---
                st.markdown("---")
                st.markdown("##### Histórico de Execuções")

                # Widgets para seleção de data (igual ao anterior)
                col_date1_hist, col_date2_hist, col_info_hist = st.columns([1, 1, 2])
                with col_date1_hist:
                    start_date_filter = st.date_input("Data Início", value=date.today(), key="exec_start_date")
                with col_date2_hist:
                    end_date_filter = st.date_input("Data Fim", value=date.today(), key="exec_end_date")

                executions = [] # Inicializa a lista de execuções
                if start_date_filter and end_date_filter:
                    if start_date_filter > end_date_filter:
                        with col_info_hist:
                            st.markdown("<div style='height: 30px;'></div>", unsafe_allow_html=True)
                            st.warning("Data de início não pode ser maior que a data fim.")
                    else:
                        # Chama a função para buscar por data (assumindo que existe e retorna lista de dicts)
                        executions = get_rule_executions_by_date(start_date_filter, end_date_filter)
                        with col_info_hist:
                            st.markdown("<div style='height: 30px;'></div>", unsafe_allow_html=True)
                            if start_date_filter == end_date_filter: st.caption(f"Exibindo execuções de {start_date_filter.strftime('%d/%m/%Y')}.")
                            else: st.caption(f"Exibindo execuções de {start_date_filter.strftime('%d/%m/%Y')} até {end_date_filter.strftime('%d/%m/%Y')}.")

                # Exibição da tabela (ou mensagem se não houver dados)
                if executions:
                    exec_df_data = []
                    # Define o fuso horário do Brasil
                    try:
                        brazil_tz = pytz.timezone('America/Sao_Paulo')
                    except pytz.UnknownTimeZoneError:
                        st.error("Erro: Fuso horário 'America/Sao_Paulo' não reconhecido pela biblioteca pytz.")
                        brazil_tz = None

                    for ex in executions:
                         if not isinstance(ex, dict): continue
                         executed_at_ts = ex.get("executed_at")
                         local_time_str = "N/A"

                         # Bloco de conversão de timezone (igual ao detalhado anteriormente)
                         if isinstance(executed_at_ts, datetime) and brazil_tz:
                            try:
                                if executed_at_ts.tzinfo is None: utc_time = pytz.utc.localize(executed_at_ts)
                                else: utc_time = executed_at_ts.astimezone(pytz.utc)
                                local_time = utc_time.astimezone(brazil_tz)
                                local_time_str = local_time.strftime('%d/%m/%Y %H:%M:%S')
                            except Exception as tz_err:
                                 print(f"AVISO: Erro ao converter timezone para execução ID {ex.get('id')}: {tz_err}")
                                 try: local_time_str = executed_at_ts.strftime('%d/%m/%Y %H:%M:%S') + " (UTC?)"
                                 except: pass

                         exec_df_data.append({
                              "Status": "✅ Sucesso" if ex.get("was_successful") else "❌ Falha",
                              "Regra": ex.get("rule_name", "N/A"),
                              "Alvo": f"{ex.get('ad_object_type', '').capitalize()}: {ex.get('ad_object_name', 'N/A')[:30]}...",
                              "Mensagem": ex.get("message", "")[:50] + ('...' if len(ex.get('message', '')) > 50 else ''),
                              "Horário (BRT)": local_time_str # Coluna com horário local
                         })

                    if exec_df_data:
                        exec_df = pd.DataFrame(exec_df_data)
                        # Ordenação pela data local (igual ao detalhado anteriormente)
                        try:
                            exec_df['Horário_DT_Local'] = pd.to_datetime(exec_df['Horário (BRT)'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
                            exec_df = exec_df.sort_values(by='Horário_DT_Local', ascending=False).drop(columns=['Horário_DT_Local'])
                        except Exception as sort_err:
                             print(f"AVISO: Não foi possível ordenar por data local: {sort_err}")
                        # Exibe o DataFrame
                        st.dataframe(exec_df, use_container_width=True, hide_index=True, height=350,
                                     column_config={"Horário (BRT)": st.column_config.TextColumn("Horário (BRT)")})
                    else:
                        st.info("Nenhuma execução válida encontrada para o período selecionado.")
                # Mensagem se não houve execuções no período (igual ao anterior)
                elif start_date_filter and end_date_filter and start_date_filter <= end_date_filter:
                     st.info(f"Nenhuma execução de regra encontrada entre {start_date_filter.strftime('%d/%m/%Y')} e {end_date_filter.strftime('%d/%m/%Y')}.")
                elif not (start_date_filter and end_date_filter):
                     st.info("Selecione as datas de início e fim para ver o histórico.")


        # ==========================
        # Aba 2: Regras
        # ==========================
        with tabs[1]:
            # ... (Cole aqui o código COMPLETO da aba de regras da sua versão anterior) ...
            # Nenhuma mudança relacionada ao timezone é necessária aqui.
            # Exemplo:
            col_rule_hdr1, col_rule_hdr2 = st.columns([3, 1])
            with col_rule_hdr2:
                # ... (botão Nova Regra/Recolher Formulário) ...
                if 'show_rule_form' not in st.session_state: st.session_state.show_rule_form = False
                button_label = "➖ Recolher Formulário" if st.session_state.show_rule_form else "➕ Nova Regra"
                # ... (lógica do botão) ...
                if st.button(button_label, key="toggle_rule_form_button", use_container_width=True, type="secondary" if st.session_state.show_rule_form else "primary"):
                    st.session_state.show_rule_form = not st.session_state.show_rule_form
                    st.rerun()


            if st.session_state.get('show_rule_form', False):
                with st.container(border=True):
                    # Assumindo que show_rule_form existe e está correta
                    show_rule_form()

            st.markdown("##### Regras Existentes")
            rules_list_tab2 = get_all_rules_cached() # Reutiliza a busca
            if rules_list_tab2:
                 # ... (Loop para exibir cada regra com botões Ativar/Excluir) ...
                 # Exemplo de como exibir a regra:
                 INTERVAL_OPTIONS_DISPLAY_TAB2 = {1: "1h", 3: "3h", 6: "6h", 12: "12h", 24: "24h"} # Exemplo
                 for rule in rules_list_tab2:
                      # ... (lógica para pegar rule_id, is_active, formatar texto da regra) ...
                      rule_id = rule.get('id')
                      if not rule_id: continue
                      is_active = bool(rule.get("is_active", False))
                      # Assumindo que format_rule_text existe
                      rule_text = format_rule_text(rule)

                      exec_mode = rule.get("execution_mode", "manual")
                      interval_h = rule.get("execution_interval_hours")
                      mode_text = ""
                      if exec_mode == 'automatic':
                          interval_desc = INTERVAL_OPTIONS_DISPLAY_TAB2.get(interval_h, f"{interval_h}h?") if interval_h else "?"
                          mode_text = f"<span style='font-size: 0.8em; color: #17a2b8;'> | Auto ({interval_desc})</span>"
                      else: mode_text = "<span style='font-size: 0.8em; color: #6c757d;'> | Manual</span>"

                      with st.container(border=True):
                          col_rule_desc, col_rule_actions = st.columns([4, 1])
                          with col_rule_desc:
                               # ... (exibe nome, texto da regra, modo) ...
                               inactive_span = '<span style="font-size: 0.8em; color: #999;">(Inativa)</span>'
                               st.markdown(f"**{rule.get('name', 'N/A')}** {inactive_span if not is_active else ''}{mode_text}", unsafe_allow_html=True)
                               st.markdown(f"<small>{rule_text}</small>", unsafe_allow_html=True)
                               if rule.get("description"): st.caption(f"Desc: {rule.get('description')}")
                          with col_rule_actions:
                               # ... (botões toggle e excluir com st.rerun e limpeza de cache) ...
                               new_toggle_state = st.toggle("Ativa", value=is_active, key=f"toggle_{rule_id}")
                               if new_toggle_state != is_active:
                                    # Assumindo que toggle_rule_status existe
                                    if toggle_rule_status(rule_id, new_toggle_state):
                                         st.toast(f"Regra '{rule.get('name')}' {'ativada' if new_toggle_state else 'desativada'}.", icon="✅" if new_toggle_state else "⏸️")
                                         if 'get_all_rules_cached' in globals(): get_all_rules_cached.clear() # Limpa cache das regras
                                         time.sleep(0.5); st.rerun()
                                    else: st.toast(f"Erro ao alterar status da regra '{rule.get('name')}'.", icon="❌")

                               if st.button("🗑️ Excluir", key=f"delete_rule_{rule_id}", type="secondary", help="Excluir", use_container_width=True):
                                    # Assumindo que delete_rule existe
                                    if delete_rule(rule_id):
                                         st.success(f"Regra '{rule.get('name')}' excluída!")
                                         if 'get_all_rules_cached' in globals(): get_all_rules_cached.clear() # Limpa cache das regras
                                         if 'get_rule_executions_cached' in globals(): get_rule_executions_cached.clear() # Limpa execuções relacionadas
                                         st.rerun()
                                    else: st.error(f"Erro ao excluir a regra '{rule.get('name')}'.")


            else:
                st.info("Nenhuma regra criada ainda. Clique em '➕ Nova Regra' para começar.")


        # ==========================
        # ==========================
        # Aba 3: Configurações
        # ==========================
        with tabs[2]:
            # --- Botão para mostrar/ocultar formulário de adicionar conta ---
            if 'show_add_config_form' not in st.session_state:
                st.session_state.show_add_config_form = False # Inicializa se não existir

            col_cfg_hdr1, col_cfg_hdr2 = st.columns([3, 1])
            with col_cfg_hdr2:
                button_label_cfg = "➖ Recolher Formulário" if st.session_state.show_add_config_form else "➕ Adicionar Conta"
                button_type_cfg = "secondary" if st.session_state.show_add_config_form else "primary"
                if st.button(button_label_cfg, key="toggle_config_form_button", use_container_width=True, type=button_type_cfg):
                    st.session_state.show_add_config_form = not st.session_state.show_add_config_form
                    st.rerun() # Recarrega para mostrar/ocultar o formulário

            # --- Formulário para Adicionar Nova Conta (se show_add_config_form for True) ---
            if st.session_state.get('show_add_config_form', False):
                with st.container(border=True): # Adiciona uma borda ao redor do formulário
                    st.markdown("##### Adicionar Nova Conta")
                    with st.form("add_api_config_form_tab3"):
                        # Campos do formulário
                        name_add = st.text_input("Nome da Conta*", help="Um nome para identificar esta conta (ex: Cliente XPTO)")
                        acc_id_add = st.text_input("Account ID* (somente números)", key="add_account_id_tab3", help="ID da sua conta de anúncios, sem 'act_' (ex: 1234567890)")
                        app_id_add = st.text_input("App ID*", key="add_app_id_tab3", help="ID do seu Aplicativo no Facebook Developers")
                        app_secret_add = st.text_input("App Secret*", type="password", key="add_app_secret_tab3", help="Chave secreta do seu App do Facebook")
                        access_token_add = st.text_area("Access Token*", key="add_access_token_tab3", height=100, help="Token de acesso de LONGA DURAÇÃO com permissões ads_read e ads_management")
                        token_expires_at_add = st.date_input(
                            "Data de Vencimento do Token", value=None, min_value=date.today(),
                            help="Selecione a data em que o token de acesso expira. Ajuda a lembrar de renovar.",
                            key="add_token_expires_at_tab3"
                        )
                        with st.expander("Configurações Opcionais"):
                            business_id_add = st.text_input("Business Manager ID", key="add_business_id_tab3", help="ID do Gerenciador de Negócios (se aplicável)")
                            page_id_add = st.text_input("Página ID Principal", key="add_page_id_tab3", help="ID da Página do Facebook principal associada (se aplicável)")

                        # Botão de salvar
                        submitted_add = st.form_submit_button("💾 Salvar Nova Conta", type="primary", use_container_width=True)
                        if submitted_add:
                            # Validações e chamada para salvar (função save_api_config)
                            if name_add and app_id_add and app_secret_add and access_token_add and acc_id_add:
                                if not acc_id_add.isdigit():
                                    st.error("Account ID deve conter apenas números.")
                                else:
                                    # Assumindo que save_api_config existe e retorna True/False
                                    if save_api_config(
                                        name_add, app_id_add, app_secret_add, access_token_add, acc_id_add,
                                        business_id_add, page_id_add, token_expires_at=token_expires_at_add
                                    ):
                                        st.success(f"Conta '{name_add}' adicionada!")
                                        st.session_state.show_add_config_form = False # Esconde formulário
                                        # Limpa caches relevantes
                                        if 'get_all_api_configs' in globals(): get_all_api_configs.clear()
                                        if 'get_active_api_config' in globals(): get_active_api_config.clear()
                                        time.sleep(1)
                                        st.rerun() # Recarrega a página
                                    else:
                                        st.error("Erro ao salvar a configuração no banco de dados.")
                            else:
                                st.warning("Preencha todos os campos marcados com *.")

                    # Expander com instruções sobre como obter credenciais
                    with st.expander("Como obter as credenciais do Facebook?"):
                         st.markdown("""
                         1.  **App ID e App Secret:** Crie um aplicativo em [Facebook for Developers](https://developers.facebook.com/apps/). Vá em Configurações > Básico.
                         2.  **Account ID (ID da Conta de Anúncios):** No Gerenciador de Anúncios do Facebook, o ID da conta aparece na URL (ex: `act=123456789`) ou nas configurações da conta. Use apenas os números.
                         3.  **Access Token (Token de Acesso):** Use a [Ferramenta do Explorer da API Graph](https://developers.facebook.com/tools/explorer/). Selecione seu App, peça um Token de Usuário com as permissões `ads_read` e `ads_management`. **Importante:** Converta este token para um token de longa duração (geralmente válido por 60 dias) usando a API ou a própria ferramenta. Cole o token de longa duração aqui.
                         4.  **Data de Vencimento do Token:** Ao gerar o token de longa duração, a API geralmente informa a data de expiração. Anote-a aqui.
                         5.  **Business Manager ID (Opcional):** Nas Configurações do Negócio do Facebook, o ID aparece na URL ou nas Informações da empresa.
                         6.  **Página ID (Opcional):** Na página do Facebook, vá na seção "Sobre" e procure pelo ID da Página.
                         """)

            # --- Exibição das Contas Configuras ---
            st.markdown("##### Suas Contas")
            # Busca todas as configs salvas (assumindo que get_all_api_configs existe)
            all_configs_tab3 = get_all_api_configs()
            if all_configs_tab3:
                # Loop para mostrar cada configuração salva
                for config in all_configs_tab3:
                    if not isinstance(config, dict): continue # Pula se não for dicionário
                    config_id = config.get('id')
                    if not config_id: continue # Pula se não tiver ID

                    is_currently_active = config.get('is_active') == 1
                    config_name = config.get('name', 'Conta sem nome') # Nome da conta

                    # Container para agrupar visualmente cada conta
                    with st.container(border=True):
                        col_details, col_actions_cfg = st.columns([4, 1]) # Colunas para detalhes e botões

                        # Coluna de Detalhes da Conta
                        with col_details:
                            # Nome da conta e badge "Ativa" se for o caso
                            active_badge = '<span class="success-badge">Ativa</span>' if is_currently_active else ''
                            st.markdown(f"**{config_name}** {active_badge}", unsafe_allow_html=True)

                            # Account ID e App ID
                            st.caption(f"Account ID: `{config.get('account_id', 'N/A')}` | App ID: `{config.get('app_id', 'N/A')}`")

                            # Data de Vencimento do Token com alertas visuais
                            expires_date = config.get('token_expires_at') # Já deve ser objeto date ou None
                            if isinstance(expires_date, date):
                                today = date.today()
                                days_left = (expires_date - today).days
                                formatted_date = expires_date.strftime('%d/%m/%Y')
                                if days_left < 0:
                                    st.caption(f"Token Expirado em: {formatted_date} ⚠️")
                                elif days_left < 7:
                                    st.caption(f"Token Vence em: {formatted_date} ({days_left} dias) ❗❗")
                                elif days_left < 30:
                                    st.caption(f"Token Vence em: {formatted_date} ({days_left} dias) ❗")
                                else:
                                    st.caption(f"Token Vence em: {formatted_date}")
                            else:
                                st.caption("Data de Vencimento do Token: Não definida")

                            # ====> AQUI ESTÁ O EXPANDER COM AS CREDENCIAIS <====
                            with st.expander("Ver/Ocultar Credenciais Sensíveis"):
                                # Campo para mostrar o App Secret (como senha)
                                st.text_input(
                                    "App Secret:",
                                    value=config.get('app_secret', 'N/A'), # Busca o valor da config
                                    type="password", # Esconde os caracteres
                                    key=f"secret_display_{config_id}", # Chave única para o widget
                                    disabled=True # Campo apenas para visualização
                                )
                                # Campo para mostrar o Access Token
                                st.text_area(
                                    "Access Token:",
                                    value=config.get('access_token', 'N/A'), # Busca o valor da config
                                    key=f"token_display_{config_id}", # Chave única para o widget
                                    disabled=True, # Campo apenas para visualização
                                    height=100 # Altura do campo de texto
                                )
                                st.caption("⚠️ Estas são informações sensíveis. Não compartilhe.")
                            # ====> FIM DO EXPANDER <====

                            # Mostra IDs opcionais se existirem
                            if config.get('business_id'):
                                st.caption(f"Business ID: `{config['business_id']}`")
                            if config.get('page_id'):
                                st.caption(f"Page ID: `{config['page_id']}`")

                        # Coluna de Ações (Botões)
                        with col_actions_cfg:
                            # Botão "Ativar" (só aparece se a conta não estiver ativa)
                            if not is_currently_active:
                                if st.button("✅ Ativar", key=f"activate_cfg_{config_id}", use_container_width=True, help=f"Tornar '{config_name}' a conta ativa"):
                                    # Assumindo que set_active_api_config existe
                                    if set_active_api_config(config_id):
                                        st.toast(f"Conta '{config_name}' ativada!", icon="✅")
                                        # Limpa caches relevantes
                                        if 'get_active_api_config' in globals(): get_active_api_config.clear()
                                        if 'get_all_api_configs' in globals(): get_all_api_configs.clear()
                                        if 'get_facebook_campaigns_cached' in globals(): get_facebook_campaigns_cached.clear()
                                        time.sleep(1)
                                        st.rerun() # Recarrega a página
                                    else:
                                        st.error("Falha ao ativar a conta.")
                            else:
                                # Adiciona um espaço vazio para alinhar com o botão excluir quando a conta está ativa
                                st.write("")

                            # Botão "Excluir"
                            if st.button("🗑️ Excluir", key=f"delete_config_{config_id}", type="secondary", use_container_width=True, help=f"Excluir permanentemente a configuração '{config_name}'"):
                                 # Assumindo que delete_api_config existe
                                 if delete_api_config(config_id):
                                     st.toast(f"Conta '{config_name}' excluída!", icon="🗑️")
                                     # Limpa caches relevantes
                                     if 'get_all_api_configs' in globals(): get_all_api_configs.clear()
                                     if 'get_active_api_config' in globals(): get_active_api_config.clear()
                                     if is_currently_active and 'get_facebook_campaigns_cached' in globals(): get_facebook_campaigns_cached.clear()
                                     time.sleep(1)
                                     st.rerun() # Recarrega a página
                                 else:
                                     st.error("Falha ao excluir a conta.")

            # Mensagem se nenhuma conta estiver configurada E o formulário não estiver visível
            elif not st.session_state.get('show_add_config_form', False):
                st.info("Nenhuma conta configurada. Clique em '➕ Adicionar Conta' para começar.")

    else:
         # Mensagem se a conexão com o DB falhou inicialmente
         st.error("🔴 **Falha na conexão com o Banco de Dados.** Verifique as variáveis de ambiente e o status do serviço PostgreSQL no Railway.")


# --- Ponto de Entrada da Página ---
# Chamado por iniciar.py
show_gerenciador_page()