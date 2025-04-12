import os
import sys
import time
from datetime import datetime, timedelta, timezone, date # Adicionado date
import traceback

# --- Tente importar as bibliotecas necessárias ---
try:
    import psycopg2
    from psycopg2 import Error as PgError
    import sqlite3 # Para verificação de tipo de erro se usar fallback
    import pandas as pd # Necessário para get_facebook_campaigns/insights
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.adaccount import AdAccount
    from facebook_business.adobjects.campaign import Campaign
    # Adicione outras importações do FB se execute_rule precisar (AdSet, Ad)
except ImportError as import_err:
    print(f"ERRO FATAL [Worker]: Biblioteca necessária não encontrada: {import_err}")
    print("Certifique-se de que todas as dependências em requirements.txt estão instaladas.")
    sys.exit(1) # Sai do script se libs essenciais faltarem

# --- Bloco de Funções Reutilizadas (ADAPTADAS PARA WORKER) ---
# Estas funções foram copiadas do seu 'gerenciador.py' e adaptadas:
# - Removido @st.cache...
# - Substituído st.error/warning/info por print()
# - Removido dependências de st.session_state

# --- Funções de Banco de Dados (ADAPTADAS PARA WORKER) ---
def get_db_connection_worker():
    """Obtém uma conexão com o banco de dados para o worker."""
    print("INFO [Worker]: Tentando obter conexão com DB...")
    pg_host = os.getenv("PGHOST")
    pg_user = os.getenv("PGUSER")
    pg_password = os.getenv("PGPASSWORD")
    pg_port = os.getenv("PGPORT")
    pg_database = os.getenv("PGDATABASE")

    if not all([pg_host, pg_user, pg_password, pg_port, pg_database]):
        print("ERRO [Worker]: Configurações do PostgreSQL não encontradas nas variáveis de ambiente.")
        # Tenta fallback SQLite como ÚLTIMO recurso (geralmente não ideal para worker)
        try:
            print("AVISO [Worker]: Tentando fallback para SQLite (não recomendado para worker)...")
            if not os.path.exists("data"): os.makedirs("data")
            conn_sqlite = sqlite3.connect("data/gcoperacional.db", timeout=10)
            print("INFO [Worker]: Conexão SQLite estabelecida como fallback.")
            return conn_sqlite, "sqlite"
        except Exception as e_sqlite:
            print(f"ERRO [Worker]: Falha ao conectar ao SQLite como fallback: {e_sqlite}")
            return None, None
    try:
        conn = psycopg2.connect(
            host=pg_host, port=pg_port, database=pg_database,
            user=pg_user, password=pg_password,
            connect_timeout=10
        )
        if conn.closed == 0:
            print("INFO [Worker]: Conexão PostgreSQL estabelecida.")
            return conn, "postgres"
        else:
            print("ERRO [Worker]: Conexão PostgreSQL foi estabelecida mas está fechada?")
            try: conn.close()
            except: pass
            return None, None
    except psycopg2.OperationalError as op_err:
         print(f"ERRO [Worker]: Falha operacional ao conectar ao PostgreSQL: {op_err}")
         return None, None
    except Exception as e:
        print(f"ERRO [Worker]: Falha GERAL ao conectar ao PostgreSQL: {e}\n{traceback.format_exc()}")
        return None, None

def execute_query(query, params=None, fetch_one=False, fetch_all=False, is_dml=False):
    """Executa uma query no banco (VERSÃO WORKER - sem st.)."""
    conn_info = get_db_connection_worker()
    if conn_info is None or conn_info[0] is None:
        print("ERRO [Worker]: execute_query - Falha ao obter conexão DB.")
        return None

    conn, conn_type = conn_info
    result = None
    cursor = None

    try:
        # Verificar se conexão ainda está aberta
        connection_ok = False
        if conn_type == 'postgres':
            connection_ok = conn.closed == 0
        elif conn_type == 'sqlite':
            try:
                conn.execute("SELECT 1") # Teste leve para SQLite
                connection_ok = True
            except:
                connection_ok = False

        if not connection_ok:
             print(f"ERRO [Worker]: execute_query - Conexão DB ({conn_type}) fechada/inválida antes de criar cursor.")
             return None

        cursor = conn.cursor()

        # Adaptar placeholders
        adapted_query = query
        if params is not None:
            if conn_type == "sqlite" and "%s" in query:
                adapted_query = query.replace("%s", "?")
            elif conn_type == "postgres" and "?" in query:
                adapted_query = query.replace("?", "%s")

        # print(f"DEBUG [Worker]: Executando Query ({conn_type}): {adapted_query} | Params: {params}")

        if params is not None:
            cursor.execute(adapted_query, params)
        else:
            cursor.execute(adapted_query)

        if is_dml:
            conn.commit()
            result = cursor.rowcount
            # print(f"DEBUG [Worker]: DML Commit ({conn_type}). Linhas afetadas: {result}")
        elif fetch_one:
            result = cursor.fetchone()
            # print(f"DEBUG [Worker]: Fetch One ({conn_type}). Resultado: {result}")
        elif fetch_all:
            result = cursor.fetchall()
            # print(f"DEBUG [Worker]: Fetch All ({conn_type}). {len(result) if result else 0} linhas.")

    except (PgError, sqlite3.Error) as db_err: # Captura erros específicos de DB primeiro
        error_type = type(db_err).__name__
        pgcode = getattr(db_err, 'pgcode', None) # Tenta pegar código de erro do Postgres
        print(f"ERRO [Worker] DB ({conn_type}, {error_type}, Code: {pgcode}): {db_err}\nQuery: {adapted_query}\nParams: {params}")
        result = None
        # Tenta rollback SE a conexão existir
        if conn:
            try:
                conn.rollback()
                print("INFO [Worker]: Rollback realizado devido a erro de DB.")
            except Exception as rb_err:
                # Erro durante o próprio rollback
                print(f"WARN [Worker]: Erro durante o rollback: {rb_err}")
    except Exception as e: # Captura outros erros inesperados
        error_type = type(e).__name__
        print(f"ERRO [Worker] GERAL na query ({error_type}): {e}\nQuery: {adapted_query}\nParams: {params}\nTraceback: {traceback.format_exc()}")
        result = None
        # Tenta rollback SE a conexão existir
        if conn:
            try:
                conn.rollback()
                print("INFO [Worker]: Rollback realizado devido a erro geral.")
            except Exception as rb_err:
                print(f"WARN [Worker]: Erro durante o rollback: {rb_err}")

    # O bloco finally continua como estava...
    finally:
        if cursor:
            try: cursor.close()
            except Exception as cur_err: print(f"WARN [Worker]: Erro ao fechar cursor: {cur_err}")
        # NÃO FECHE A CONEXÃO AQUI

    return result

def get_active_api_config():
    """Obtém a configuração ativa (VERSÃO WORKER - sem st.)."""
    print("INFO [Worker]: Buscando configuração de API ativa...")
    # Usa is_active=1 no WHERE (INTEGER 1 para True)
    query = """
        SELECT id, name, app_id, app_secret, access_token, account_id,
               business_id, page_id, token_expires_at
        FROM api_config WHERE is_active = 1 LIMIT 1
    """
    row = execute_query(query, fetch_one=True) # Usa a execute_query adaptada
    if row:
        keys = ["id", "name", "app_id", "app_secret", "access_token", "account_id",
                "business_id", "page_id", "token_expires_at"]
        config_dict = dict(zip(keys, row))
        print(f"INFO [Worker]: Configuração ativa encontrada: ID {config_dict.get('id')}, Nome: {config_dict.get('name')}")
        return config_dict
    else:
        print("AVISO [Worker]: Nenhuma configuração de API ativa encontrada no banco de dados.")
        return None

# --- Funções da API do Facebook (ADAPTADAS PARA WORKER) ---
def init_facebook_api_worker():
    """Inicializa a API do Facebook (VERSÃO WORKER - sem st.)."""
    print("INFO [Worker]: Inicializando API do Facebook...")
    config = get_active_api_config()
    if not config:
        # Mensagem já impressa por get_active_api_config
        return None

    required_keys = ["app_id", "app_secret", "access_token", "account_id"]
    missing_keys = [key for key in required_keys if not config.get(key)]
    if missing_keys:
         print(f"ERRO [Worker]: Configuração ativa '{config.get('name', 'N/A')}' incompleta. Faltam: {', '.join(missing_keys)}.")
         return None

    # Verifica expiração do token
    expires_date = config.get('token_expires_at') # Vem como date ou None
    if isinstance(expires_date, date):
        print(f"INFO [Worker]: Verificando expiração do token. Expira em: {expires_date}, Hoje: {date.today()}")
        if expires_date < date.today():
            print(f"ERRO [Worker]: Token de Acesso para conta '{config.get('name')}' expirou em {expires_date.strftime('%d/%m/%Y')}.")
            return None
    else:
        print(f"AVISO [Worker]: Data de expiração do token para '{config.get('name')}' não definida.")

    try:
        print(f"INFO [Worker]: Inicializando FacebookAdsApi com App ID: {config['app_id']}...")
        FacebookAdsApi.init(
            app_id=config["app_id"],
            app_secret=config["app_secret"],
            access_token=config["access_token"],
            api_version='v20.0' # Use a versão correta
        )
        print("INFO [Worker]: FacebookAdsApi inicializada.")

        account_str_id = f'act_{config["account_id"]}'
        print(f"INFO [Worker]: Verificando conexão com a conta {account_str_id}...")
        try:
            AdAccount(account_str_id).api_get(fields=['id'])
            print(f"INFO [Worker]: Conexão com conta {account_str_id} OK.")
            return config["account_id"] # Retorna o ID da conta se sucesso
        except Exception as conn_err:
             print(f"ERRO [Worker]: Falha ao verificar conexão com conta {account_str_id}: {conn_err}.")
             print("    -> Verifique Token, Permissões (ads_read, ads_management) e Account ID.")
             return None
    except Exception as e:
        print(f"ERRO [Worker]: Falha CRÍTICA ao inicializar API FB: {e}\n{traceback.format_exc()}")
        return None

def get_campaign_insights(account_id, campaign_ids_list, time_range='last_7d'):
     """Busca insights (VERSÃO WORKER - sem cache)."""
     # (Copiado de gerenciador.py, removido @st.cache_data, adaptado logs)
     if not account_id or not campaign_ids_list:
        print("WARN [Worker insights]: Account ID ou lista de IDs de campanha vazia.")
        return []
     print(f"INFO [Worker insights]: Buscando insights para {len(campaign_ids_list)} campanhas (conta {account_id}, período {time_range})...")
     try:
        params = {
            'level': 'campaign',
            'filtering': [{'field': 'campaign.id', 'operator': 'IN', 'value': campaign_ids_list}],
            'breakdowns': []
        }
        if time_range == 'yesterday':
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            params['time_range'] = {'since': yesterday, 'until': yesterday}
        elif time_range == 'last_7d': params['date_preset'] = 'last_7d'
        elif time_range == 'last_30d': params['date_preset'] = 'last_30d'
        else: params['date_preset'] = 'last_7d' # Default

        account = AdAccount(f'act_{account_id}')
        insights = account.get_insights(
            params=params,
            fields=[ # Campos essenciais para as regras
                'campaign_id', 'campaign_name', 'spend', 'impressions', 'clicks',
                'ctr', 'cpc', 'actions', 'cost_per_action_type', 'purchase_roas'
            ]
        )
        processed_insights = []
        for insight in insights:
            insight_dict = insight.export_all_data()
            purchases = 0
            purchase_value = 0.0
            if 'actions' in insight_dict:
                for action in insight_dict['actions']:
                    # Simplificado para pegar qualquer tipo de compra principal
                    if 'purchase' in action.get('action_type', ''):
                         # Tenta pegar valor de compra mais específico primeiro
                         action_values = action.get('action_values')
                         if isinstance(action_values, list) and len(action_values) > 0:
                             purchase_value += float(action_values[0].get('value', 0.0))
                         else: # Fallback para o valor geral da ação
                             purchase_value += float(action.get('value', 0.0))
                         # Conta as compras (pode ser impreciso se houver vários tipos 'purchase')
                         purchases += int(float(action.get('value', 0))) # Tenta converter valor para int


            # Extrai CPA de compra (pode precisar de ajuste fino nos action_types)
            cpa = 0.0
            if 'cost_per_action_type' in insight_dict:
                for cost_action in insight_dict['cost_per_action_type']:
                    if 'purchase' in cost_action.get('action_type', ''):
                        cpa = float(cost_action.get('value', 0.0))
                        break # Pega o primeiro CPA de compra encontrado

            # Extrai ROAS
            roas = 0.0
            if 'purchase_roas' in insight_dict:
                 roas_list = insight_dict['purchase_roas']
                 if roas_list and isinstance(roas_list, list) and len(roas_list) > 0:
                     roas = float(roas_list[0].get('value', 0.0))

            insight_dict['purchases'] = purchases
            insight_dict['cpa'] = cpa
            insight_dict['roas'] = roas
            insight_dict['purchase_value'] = purchase_value # Valor total de compra

            # Adiciona outras métricas importantes que já vêm
            insight_dict['spend'] = float(insight_dict.get('spend', 0.0))
            insight_dict['clicks'] = int(insight_dict.get('clicks', 0))
            insight_dict['impressions'] = int(insight_dict.get('impressions', 0))
            insight_dict['ctr'] = float(insight_dict.get('ctr', 0.0))
            insight_dict['cpc'] = float(insight_dict.get('cpc', 0.0))

            processed_insights.append(insight_dict)
        print(f"INFO [Worker insights]: {len(processed_insights)} insights processados.")
        return processed_insights
     except Exception as e:
        print(f"ERRO [Worker insights]: Falha ao obter insights para campanhas {','.join(campaign_ids_list)}: {e}")
        print(f"Traceback: {traceback.format_exc()}")
        return [] # Retorna lista vazia em caso de erro

def get_facebook_campaigns(account_id_from_worker):
    """Busca campanhas e insights (VERSÃO WORKER - sem cache)."""
    # (Copiado de gerenciador.py, removido @st.cache_data, chama init_facebook_api_worker e get_campaign_insights, adaptado logs)
    print(f"INFO [Worker campaigns]: Buscando campanhas da conta {account_id_from_worker}...")
    campaigns_result = []
    try:
        # API já deve estar inicializada por init_facebook_api_worker chamado antes
        # Mas uma verificação extra pode ser útil
        if not FacebookAdsApi.get_default_api():
             print("ERRO [Worker campaigns]: API do Facebook não inicializada.")
             return None

        account = AdAccount(f'act_{account_id_from_worker}')
        fields_to_fetch = [
            'id', 'name', 'status', 'objective', 'created_time',
            'start_time', 'stop_time', 'daily_budget', 'lifetime_budget',
            'effective_status', # Importante para saber se está realmente ativa/pausada
            'buying_type', 'budget_remaining'
        ]
        # Aumenta o limite para buscar mais campanhas se necessário
        campaigns_raw = list(account.get_campaigns(fields=fields_to_fetch, params={'limit': 500, 'filtering': "[{'field':'effective_status','operator':'IN','value':['ACTIVE','PAUSED','PENDING_REVIEW','WITH_ISSUES','DISAPPROVED']}]"})) # Filtra status relevantes

        if not campaigns_raw:
             print("INFO [Worker campaigns]: Nenhuma campanha encontrada (ou nenhuma com status relevante).")
             return []

        campaign_ids = [campaign.get("id") for campaign in campaigns_raw if campaign.get("id")]
        if not campaign_ids: return []
        print(f"INFO [Worker campaigns]: {len(campaign_ids)} campanhas encontradas. Buscando insights...")

        # Chama a versão worker de get_campaign_insights
        insights_data = get_campaign_insights(account_id_from_worker, campaign_ids, "last_7d") # Usando last_7d como padrão
        insights_map = {insight.get("campaign_id"): insight for insight in insights_data if insight.get("campaign_id")}
        print(f"INFO [Worker campaigns]: {len(insights_map)} insights encontrados.")

        for campaign in campaigns_raw:
            campaign_dict = campaign.export_all_data()
            campaign_id = campaign_dict.get("id")
            if not campaign_id: continue

            campaign_insights = insights_map.get(campaign_id)
            insights_default = {
                "cpa": 0.0, "purchases": 0, "roas": 0.0, "purchase_value": 0.0,
                "spend": 0.0, "clicks": 0, "impressions": 0, "ctr": 0.0, "cpc": 0.0
            }
            # Combina insights ou usa default
            campaign_dict["insights"] = {**insights_default, **(campaign_insights or {})}

            # Converte orçamento para centavos (int)
            daily_budget_str = campaign_dict.get('daily_budget')
            lifetime_budget_str = campaign_dict.get('lifetime_budget')
            campaign_dict['daily_budget'] = int(daily_budget_str) if daily_budget_str and daily_budget_str.isdigit() else 0
            campaign_dict['lifetime_budget'] = int(lifetime_budget_str) if lifetime_budget_str and lifetime_budget_str.isdigit() else 0

            campaigns_result.append(campaign_dict)

        print(f"INFO [Worker campaigns]: {len(campaigns_result)} campanhas com insights processadas.")
        return campaigns_result

    except Exception as e:
        print(f"ERRO CRÍTICO [Worker campaigns]: Falha ao buscar campanhas/insights: {e}")
        print(f"Traceback: {traceback.format_exc()}")
        return None # Retorna None para indicar erro


# --- Funções de Regras (ADAPTADAS PARA WORKER) ---
def simulate_rule_application(campaign, rules):
    """Verifica quais regras ativas teriam suas condições atendidas (VERSÃO WORKER - sem st.)."""
    # (Copiado de gerenciador.py - geralmente não precisa de adaptação significativa)
    rule_results = []
    if not campaign or not isinstance(campaign, dict) or "insights" not in campaign:
        # print("DEBUG [Simulate]: Campanha inválida ou sem insights.")
        return []

    # Extrai métricas da campanha (garante que são float/int)
    insights = campaign.get("insights", {})
    metrics = {
        'cpa': float(insights.get("cpa", 0.0)),
        'purchases': int(insights.get("purchases", 0)),
        'roas': float(insights.get("roas", 0.0)),
        'spend': float(insights.get("spend", 0.0)),
        'clicks': int(insights.get("clicks", 0)),
        'ctr': float(insights.get("ctr", 0.0)),
        'cpc': float(insights.get("cpc", 0.0)),
    }
    current_daily_budget = int(campaign.get('daily_budget', 0))
    current_lifetime_budget = int(campaign.get('lifetime_budget', 0))
    current_budget = current_daily_budget if current_daily_budget > 0 else current_lifetime_budget

    for rule in rules:
        # Worker só deve simular regras ativas passadas para ele
        if not isinstance(rule, dict) or not rule.get('is_active', 1): continue # Pula se não for dict ou inativa

        primary_metric_key = rule.get('primary_metric')
        primary_operator = rule.get('primary_operator')
        primary_value_rule = rule.get('primary_value') # Vem como float/real do DB

        if None in [primary_metric_key, primary_operator] or primary_value_rule is None or primary_metric_key not in metrics:
            # print(f"DEBUG [Simulate Rule {rule.get('id')}]: Condição primária inválida ou métrica não encontrada.")
            continue

        primary_value_campaign = metrics[primary_metric_key]
        primary_condition_met = False
        try:
            # Comparação direta (ambos devem ser numéricos)
            if primary_operator == '<' and primary_value_campaign < primary_value_rule: primary_condition_met = True
            elif primary_operator == '<=' and primary_value_campaign <= primary_value_rule: primary_condition_met = True
            elif primary_operator == '>' and primary_value_campaign > primary_value_rule: primary_condition_met = True
            elif primary_operator == '>=' and primary_value_campaign >= primary_value_rule: primary_condition_met = True
            elif primary_operator == '==' and primary_value_campaign == primary_value_rule: primary_condition_met = True
        except TypeError as te:
            print(f"WARN [Simulate Rule {rule.get('id')}]: Erro de tipo na comparação primária - Campanha: {primary_value_campaign}, Regra: {primary_value_rule}. {te}")
            continue

        condition_met = primary_condition_met

        if rule.get('is_composite'): # is_composite é 0 ou 1
            secondary_metric_key = rule.get('secondary_metric')
            secondary_operator = rule.get('secondary_operator')
            secondary_value_rule = rule.get('secondary_value') # Vem como float/real
            join_operator = rule.get('join_operator', 'AND')

            if None in [secondary_metric_key, secondary_operator] or secondary_value_rule is None or secondary_metric_key not in metrics:
                 if join_operator == 'AND': condition_met = False # Se AND, falha se secundária for inválida
                 # Se OR, o resultado da primária ainda vale
            else:
                secondary_value_campaign = metrics[secondary_metric_key]
                secondary_condition_met = False
                try:
                    # Comparação secundária
                    if secondary_operator == '<' and secondary_value_campaign < secondary_value_rule: secondary_condition_met = True
                    elif secondary_operator == '<=' and secondary_value_campaign <= secondary_value_rule: secondary_condition_met = True
                    elif secondary_operator == '>' and secondary_value_campaign > secondary_value_rule: secondary_condition_met = True
                    elif secondary_operator == '>=' and secondary_value_campaign >= secondary_value_rule: secondary_condition_met = True
                    elif secondary_operator == '==' and secondary_value_campaign == secondary_value_rule: secondary_condition_met = True
                except TypeError as te2:
                    print(f"WARN [Simulate Rule {rule.get('id')}]: Erro de tipo na comparação secundária - Campanha: {secondary_value_campaign}, Regra: {secondary_value_rule}. {te2}")
                    secondary_condition_met = False # Considera falso se der erro

                if join_operator == 'AND': condition_met = primary_condition_met and secondary_condition_met
                elif join_operator == 'OR': condition_met = primary_condition_met or secondary_condition_met
                else: condition_met = False # Operador desconhecido

        if condition_met:
            action_type = rule.get('action_type')
            action_value = rule.get('action_value') # Vem como float/real
            action_text = ""
            new_budget_simulated = None
            min_budget_cents = 100 # Orçamento mínimo (ex: R$ 1,00)

            # Lógica para determinar texto da ação e orçamento simulado
            if action_type == 'duplicate_budget':
                action_text = "Duplicar orçamento"
                if current_budget > 0: new_budget_simulated = max(min_budget_cents, current_budget * 2)
            elif action_type == 'triple_budget':
                action_text = "Triplicar orçamento"
                if current_budget > 0: new_budget_simulated = max(min_budget_cents, current_budget * 3)
            elif action_type == 'pause_campaign': action_text = "Pausar campanha"
            elif action_type == 'activate_campaign': action_text = "Ativar campanha"
            elif action_type == 'halve_budget':
                action_text = "Reduzir orçamento pela metade"
                if current_budget > 0: new_budget_simulated = max(min_budget_cents, current_budget // 2)
            elif action_type == 'custom_budget_multiplier' and action_value is not None:
                 multiplier = float(action_value) # Já deve ser float
                 action_text = f"Multiplicar orçamento por {multiplier:.2f}"
                 if current_budget > 0: new_budget_simulated = max(min_budget_cents, int(current_budget * multiplier))
            elif action_type == 'custom_budget_multiplier' and action_value is None:
                 action_text = "Multiplicar orçamento (VALOR NÃO DEFINIDO!)"
            else:
                 action_text = f"Ação desconhecida ({action_type})"

            rule_results.append({
                "rule_id": rule.get('id'),
                "rule_name": rule.get('name', 'Regra sem nome'),
                "action": action_text,
                "new_budget_simulated": new_budget_simulated # Pode ser None
            })

    return rule_results

def log_rule_execution(rule_id, ad_object_id, ad_object_type, ad_object_name, was_successful, message=""):
    """Registra a execução de uma regra no banco (VERSÃO WORKER - sem st.)."""
    # (Copiado de gerenciador.py, adaptado para usar execute_query worker)
    print(f"INFO [Log exec]: RuleID:{rule_id}, ObjID:{ad_object_id}, Type:{ad_object_type}, Success:{was_successful}, Msg:'{message[:50]}...'")

    # Usa execute_query para inserir, tratando placeholders automaticamente
    query = """
        INSERT INTO rule_executions
        (rule_id, ad_object_id, ad_object_type, ad_object_name, was_successful, message, executed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    # Garante que was_successful seja 0 ou 1
    success_flag = 1 if was_successful else 0
    # Usa now_utc para consistência de fuso horário
    now_utc_log = datetime.now(timezone.utc)
    params = (rule_id, ad_object_id, ad_object_type, ad_object_name, success_flag, message, now_utc_log)

    insert_result = execute_query(query, params, is_dml=True) # Usa a execute_query adaptada

    if insert_result is None or insert_result == 0:
        print(f"ERRO [Log exec]: Falha ao inserir log para regra ID {rule_id}, objeto {ad_object_id}.")
        return False
    else:
        # print(f"DEBUG [Log exec]: Log inserido com sucesso.")
        return True

def execute_rule(campaign_id, rule_id):
    """Executa a ação definida por uma regra (VERSÃO WORKER - sem st.)."""
    # (Copiado de gerenciador.py, adaptado para usar funções worker e logging)
    print(f"INFO [Exec Rule]: Tentando executar Regra ID {rule_id} na Campanha ID {campaign_id}")
    campaign_name = f'Campanha ID {campaign_id}' # Nome default
    success = False
    message = ""
    rule = None # Para garantir que rule está definida

    try:
        # 1. Buscar a regra específica no DB (não precisamos de todas)
        query_rule = """
            SELECT id, name, action_type, action_value, is_active
            FROM rules WHERE id = %s
        """
        # execute_query retorna tupla ou None
        rule_data = execute_query(query_rule, (rule_id,), fetch_one=True)
        if not rule_data:
            message = f"Regra ID {rule_id} não encontrada no banco de dados."
            print(f"ERRO [Exec Rule]: {message}")
            log_rule_execution(rule_id, campaign_id, 'campaign', campaign_name, False, message)
            return False, message

        # Mapeia para dict
        rule_keys_exec = ["id", "name", "action_type", "action_value", "is_active"]
        rule = dict(zip(rule_keys_exec, rule_data))

        # 2. Verificar se a regra está ativa
        if not rule.get('is_active'):
            message = f"Regra '{rule.get('name')}' (ID: {rule_id}) está inativa."
            print(f"AVISO [Exec Rule]: {message}")
            # Não logamos como falha de execução, apenas não executamos
            return False, message # Indica que não executou, mas não foi um erro

        # 3. Garantir que a API está inicializada (embora já deva estar)
        if not FacebookAdsApi.get_default_api():
             message = "API do Facebook não inicializada antes de executar regra."
             print(f"ERRO [Exec Rule]: {message}")
             log_rule_execution(rule_id, campaign_id, 'campaign', rule.get('name', 'N/A'), False, message)
             return False, message

        # 4. Obter dados atuais da campanha via API
        try:
            print(f"  -> Buscando dados da campanha {campaign_id} via API...")
            campaign_obj = Campaign(campaign_id)
            # Campos necessários para aplicar ações e logar nome
            campaign_data = campaign_obj.api_get(fields=['name', 'status', 'daily_budget', 'lifetime_budget', 'effective_status'])
            campaign_name = campaign_data.get('name', campaign_name) # Atualiza nome
            print(f"  -> Dados encontrados para '{campaign_name}'. Status: {campaign_data.get('effective_status')}")
        except Exception as api_get_err:
            message = f"Erro ao buscar dados da campanha {campaign_id} na API: {api_get_err}"
            print(f"ERRO [Exec Rule]: {message}")
            log_rule_execution(rule_id, campaign_id, 'campaign', campaign_name, False, message)
            return False, message

        # 5. Determinar e aplicar a ação
        action_params = {} # Parâmetros para a chamada api_update
        current_daily_budget = int(campaign_data.get('daily_budget', 0))
        current_lifetime_budget = int(campaign_data.get('lifetime_budget', 0))
        current_status = campaign_data.get('status') # Status configurado (ACTIVE/PAUSED)
        effective_status = campaign_data.get('effective_status') # Status real (pode ser afetado por conta, adset, etc.)
        min_budget_cents = 100 # R$ 1,00

        action_type = rule['action_type']
        action_value = rule.get('action_value') # Vem como float/real

        if action_type == 'duplicate_budget':
            if current_daily_budget > 0:
                new_budget = max(min_budget_cents, current_daily_budget * 2)
                action_params = {'daily_budget': new_budget}
                message = f"Orçamento diário duplicado para {new_budget/100:.2f}"
            elif current_lifetime_budget > 0:
                new_budget = max(min_budget_cents, current_lifetime_budget * 2)
                action_params = {'lifetime_budget': new_budget}
                message = f"Orçamento total duplicado para {new_budget/100:.2f}"
            else: message = "Nenhum orçamento (diário/total) encontrado para duplicar"; action_params=None; success=False # Não executa API

        elif action_type == 'triple_budget':
            if current_daily_budget > 0:
                new_budget = max(min_budget_cents, current_daily_budget * 3)
                action_params = {'daily_budget': new_budget}
                message = f"Orçamento diário triplicado para {new_budget/100:.2f}"
            elif current_lifetime_budget > 0:
                new_budget = max(min_budget_cents, current_lifetime_budget * 3)
                action_params = {'lifetime_budget': new_budget}
                message = f"Orçamento total triplicado para {new_budget/100:.2f}"
            else: message = "Nenhum orçamento encontrado para triplicar"; action_params=None; success=False

        elif action_type == 'pause_campaign':
            if current_status == Campaign.Status.active:
                action_params = {'status': Campaign.Status.paused}
                message = "Campanha pausada"
            else: # Se já está pausada ou outro status, considera sucesso (não fazer nada)
                 message = f"Campanha já estava com status '{current_status}'. Nenhuma ação necessária."
                 action_params=None; success = True # Não executa API, mas loga como sucesso

        elif action_type == 'activate_campaign':
             if current_status == Campaign.Status.paused:
                action_params = {'status': Campaign.Status.active}
                message = "Campanha ativada"
             else: # Se já ativa ou outro status, considera sucesso
                 message = f"Campanha já estava com status '{current_status}'. Nenhuma ação necessária."
                 action_params=None; success = True # Não executa API, mas loga como sucesso

        elif action_type == 'halve_budget':
            if current_daily_budget > 0:
                new_budget = max(min_budget_cents, current_daily_budget // 2)
                action_params = {'daily_budget': new_budget}
                message = f"Orçamento diário reduzido para {new_budget/100:.2f}"
            elif current_lifetime_budget > 0:
                new_budget = max(min_budget_cents, current_lifetime_budget // 2)
                action_params = {'lifetime_budget': new_budget}
                message = f"Orçamento total reduzido para {new_budget/100:.2f}"
            else: message = "Nenhum orçamento encontrado para reduzir"; action_params=None; success=False

        elif action_type == 'custom_budget_multiplier':
            if action_value is None or action_value <= 0:
                 message = f"Multiplicador de orçamento inválido ({action_value}) na regra."
                 action_params=None; success = False
            else:
                multiplier = float(action_value) # Garante float
                if current_daily_budget > 0:
                    new_budget = max(min_budget_cents, int(current_daily_budget * multiplier))
                    action_params = {'daily_budget': new_budget}
                    message = f"Orçamento diário multiplicado por {multiplier:.2f} para {new_budget/100:.2f}"
                elif current_lifetime_budget > 0:
                    new_budget = max(min_budget_cents, int(current_lifetime_budget * multiplier))
                    action_params = {'lifetime_budget': new_budget}
                    message = f"Orçamento total multiplicado por {multiplier:.2f} para {new_budget/100:.2f}"
                else: message = "Nenhum orçamento encontrado para multiplicar"; action_params=None; success=False
        else:
             message = f"Tipo de ação desconhecido ou inválido na regra: {action_type}"; action_params=None; success = False

        # 6. Executar chamada à API se houver parâmetros
        if action_params:
            print(f"  -> Aplicando Ação API: {action_params} para Campanha ID {campaign_id}")
            try:
                campaign_obj.api_update(params=action_params)
                success = True
                print(f"  -> Ação API aplicada com sucesso.")
                # Mensagem já foi definida acima
            except Exception as api_update_err:
                message = f"Erro da API ao aplicar ação '{action_type}': {api_update_err}"
                print(f"ERRO [Exec Rule]: {message}")
                success = False
        elif success: # Caso onde a ação era "não fazer nada" e foi considerado sucesso
             print(f"  -> Nenhuma chamada API necessária: {message}")
        else: # Caso onde a ação não pôde ser determinada (ex: sem orçamento)
             print(f"  -> Nenhuma chamada API executada: {message}")


    except Exception as e:
        message = f"Erro inesperado durante execução da regra ID {rule_id} na campanha {campaign_id}: {str(e)}"
        print(f"ERRO CRÍTICO [Exec Rule]: {message}\n{traceback.format_exc()}")
        success = False

    # 7. Logar o resultado final
    log_rule_execution(
        rule_id=rule_id, ad_object_id=campaign_id, ad_object_type='campaign',
        ad_object_name=campaign_name, was_successful=success, message=message
    )
    return success, message


# --- Função Principal do Worker ---
def run_automatic_rules():
    """Verifica e executa regras automáticas agendadas."""
    start_time = time.time()
    print(f"\n--- [WORKER START] {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')} ---")
    main_conn, db_type = get_db_connection_worker()
    if not main_conn:
        print("ERRO FATAL [Worker]: Não foi possível conectar ao banco de dados principal. Abortando.")
        return

    active_account_id = None
    all_campaigns_data = []
    processed_rules_count = 0
    executed_actions_count = 0

    try:
        # 1. Inicializar API (usa funções adaptadas)
        active_account_id = init_facebook_api_worker()
        if not active_account_id:
            print("AVISO [Worker]: Falha ao inicializar API ou nenhuma conta ativa. Abortando ciclo.")
            return

        # 2. Buscar campanhas (usa funções adaptadas)
        all_campaigns_data = get_facebook_campaigns(active_account_id)
        if all_campaigns_data is None: # Indica erro na busca
             print("ERRO [Worker]: Falha ao buscar campanhas da API. Abortando ciclo.")
             return
        # Filtra campanhas elegíveis (não arquivadas/deletadas)
        campaigns_to_check = [c for c in all_campaigns_data if isinstance(c, dict) and c.get('effective_status') not in ['ARCHIVED', 'DELETED']]
        print(f"INFO [Worker]: {len(campaigns_to_check)} campanhas elegíveis encontradas.")
        if not campaigns_to_check: print("AVISO [Worker]: Nenhuma campanha elegível para aplicar regras neste ciclo.")

        # 3. Buscar regras automáticas e ativas (usa execute_query adaptada)
        print("INFO [Worker]: Buscando regras automáticas ativas...")
        query = """
            SELECT id, name, execution_interval_hours, last_automatic_run_at,
                   is_composite, primary_metric, primary_operator, primary_value,
                   secondary_metric, secondary_operator, secondary_value, join_operator,
                   action_type, action_value
            FROM rules
            WHERE execution_mode = 'automatic' AND is_active = 1
            ORDER BY id
        """
        rules_data = execute_query(query, fetch_all=True)

        if rules_data is None:
            print("ERRO [Worker]: Falha ao buscar regras do banco de dados.")
            return
        if not rules_data:
            print("INFO [Worker]: Nenhuma regra automática ativa encontrada para processar.")
            return

        print(f"INFO [Worker]: {len(rules_data)} regras automáticas ativas encontradas.")
        now_utc = datetime.now(timezone.utc)

        rule_keys = ["id", "name", "execution_interval_hours", "last_automatic_run_at", "is_composite", "primary_metric", "primary_operator", "primary_value", "secondary_metric", "secondary_operator", "secondary_value", "join_operator", "action_type", "action_value"]

        # 4. Iterar e verificar cada regra
        for rule_tuple in rules_data:
            rule = dict(zip(rule_keys, rule_tuple))
            rule_id = rule['id']
            rule_name = rule['name']
            interval_h = rule.get('execution_interval_hours')
            last_run_ts = rule.get('last_automatic_run_at')

            print(f"\n---> Verificando Regra ID {rule_id} ('{rule_name}')")
            processed_rules_count += 1

            if not interval_h or interval_h <= 0:
                print(f"  AVISO: Intervalo inválido ({interval_h}h). Pulando regra.")
                continue

            is_due = False
            if last_run_ts is None:
                is_due = True
                print(f"  INFO: Primeira execução automática.")
            else:
                if last_run_ts.tzinfo is None: last_run_ts = last_run_ts.replace(tzinfo=timezone.utc)
                next_due_time = last_run_ts + timedelta(hours=interval_h)
                print(f"  INFO: Última: {last_run_ts.strftime('%Y-%m-%d %H:%M %Z')}, Próxima >= {next_due_time.strftime('%Y-%m-%d %H:%M %Z')}")
                if now_utc >= next_due_time:
                    is_due = True
                    print(f"  INFO: Horário atual ({now_utc.strftime('%Y-%m-%d %H:%M %Z')}) atingido. Regra pronta.")
                else:
                    print(f"  INFO: Ainda não é hora.")

            if is_due:
                print(f"  EXECUTANDO Regra ID {rule_id}...")
                cycle_processed = False # Flag para saber se atualiza o timestamp

                if not campaigns_to_check:
                    print("  INFO: Nenhuma campanha elegível para testar.")
                    cycle_processed = True # Marcar como processado mesmo sem campanhas
                else:
                    rule['is_active'] = 1 # Necessário para simulate_rule_application
                    for campaign in campaigns_to_check:
                        campaign_id = campaign.get('id')
                        campaign_name = campaign.get('name', f"ID {campaign_id}")
                        try:
                            # Simula ANTES de executar
                            sim_results = simulate_rule_application(campaign, [rule])
                            if sim_results and any(s.get('rule_id') == rule_id for s in sim_results):
                                print(f"    -> Condição atendida para Campanha ID {campaign_id} ('{campaign_name[:30]}...'). Executando...")
                                success_exec, msg_exec = execute_rule(campaign_id, rule_id)
                                if success_exec: executed_actions_count += 1
                                # execute_rule já faz o log interno
                            # else: print(f"    -> Condição NÃO atendida para Campanha ID {campaign_id}.")
                            cycle_processed = True # Marcar como processado se tentou simular/executar
                        except Exception as sim_exec_err:
                             print(f"    -> ERRO CRÍTICO durante sim/exec para Campanha ID {campaign_id}: {sim_exec_err}")
                             log_rule_execution(rule_id, campaign_id, 'campaign', campaign_name, False, f"Worker sim/exec error: {str(sim_exec_err)[:150]}")
                             cycle_processed = True # Marcar como processado mesmo com erro

                # 5. Atualizar last_automatic_run_at SE o ciclo foi processado
                if cycle_processed:
                    print(f"  INFO: Atualizando 'last_automatic_run_at' para regra ID {rule_id}...")
                    update_result = execute_query(
                        "UPDATE rules SET last_automatic_run_at = %s, updated_at = %s WHERE id = %s",
                        (now_utc, now_utc, rule_id), # Atualiza ambos os timestamps
                        is_dml=True
                    )
                    if update_result is None or update_result == 0:
                         print(f"  ERRO: Falha ao atualizar timestamp para regra ID {rule_id}.")
                    # else: print(f"  INFO: Timestamp atualizado.")
            # else: Regra não 'due'
    except KeyboardInterrupt:
         print("\nWARN [Worker]: Execução interrompida manualmente (Ctrl+C).")
    except Exception as e:
        print(f"ERRO CRÍTICO INESPERADO [Worker]: {e}\n{traceback.format_exc()}")
    finally:
        if main_conn:
            try:
                main_conn.close()
                print("INFO [Worker]: Conexão principal com DB fechada.")
            except Exception as close_err:
                print(f"ERRO [Worker]: Falha ao fechar conexão DB: {close_err}")

        end_time = time.time()
        duration = end_time - start_time
        print(f"--- [WORKER END] {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')} ---")
        print(f"Tempo total: {duration:.2f} seg. Regras processadas: {processed_rules_count}. Ações executadas: {executed_actions_count}.")


# --- Ponto de Entrada do Script ---
if __name__ == "__main__":
    # Verifica variáveis de ambiente essenciais
    required_env_vars = ["PGHOST", "PGUSER", "PGPASSWORD", "PGPORT", "PGDATABASE"]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        print(f"ERRO FATAL [Worker]: Variáveis de ambiente faltando: {', '.join(missing_vars)}")
        print("Configure-as no Railway (Service Variables).")
        sys.exit(1)

    print(f"INFO [Worker]: Iniciando execução do script {os.path.basename(__file__)}")
    run_automatic_rules()
    print(f"INFO [Worker]: Script {os.path.basename(__file__)} concluído.")