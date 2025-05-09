import os
import sys
import time
from datetime import datetime, timedelta, timezone, date
import traceback

try:
    import psycopg2
    from psycopg2 import Error as PgError
    import sqlite3
    import pandas as pd
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.adaccount import AdAccount
    from facebook_business.adobjects.campaign import Campaign
except ImportError as import_err:
    print(f"ERRO FATAL [Worker]: Biblioteca necessária não encontrada: {import_err}")
    sys.exit(1)

# --- Bloco de Funções Reutilizadas (ADAPTADAS PARA WORKER) ---
# <<<======================================================================>>>
# <<<  COLE AQUI AS VERSÕES COMPLETAS E ADAPTADAS (SEM st., SEM CACHE)     >>>
# <<<  DAS FUNÇÕES: get_db_connection_worker, execute_query,               >>>
# <<<  get_active_api_config (RENOMEAR para get_api_config_by_id talvez?),>>>
# <<<  get_all_api_configs_worker (NOVA), init_facebook_api_worker,        >>>
# <<<  get_campaign_insights, get_facebook_campaigns,                      >>>
# <<<  simulate_rule_application, log_rule_execution, execute_rule        >>>
# <<<======================================================================>>>

# Exemplo de get_db_connection_worker (COLE A SUA VERSÃO COMPLETA)
def get_db_connection_worker():
    """Obtém uma conexão com o banco de dados (VERSÃO WORKER)."""
    print("INFO [Worker DB]: Tentando obter conexão com DB...")
    pg_host = os.getenv("PGHOST")
    pg_user = os.getenv("PGUSER")
    pg_password = os.getenv("PGPASSWORD")
    pg_port = os.getenv("PGPORT") # Garanta que PGPORT está presente
    pg_database = os.getenv("PGDATABASE") # Garanta que PGDATABASE está presente

    # Verifica se todas as variáveis PG existem
    if not all([pg_host, pg_user, pg_password, pg_port, pg_database]):
        print("ERRO [Worker DB]: Faltam variáveis de ambiente PostgreSQL (PGHOST, PGUSER, PGPASSWORD, PGPORT, PGDATABASE).")
        # Tentar SQLite como fallback? Ou apenas falhar? Vamos falhar por clareza.
        # Remover bloco SQLite se não for usá-lo no worker.
        # try:
        #     print("AVISO [Worker DB]: Tentando fallback para SQLite...")
        #     # ... (lógica SQLite se necessário) ...
        # except Exception as e_sql:
        #      print(f"ERRO [Worker DB]: Falha ao conectar ao SQLite: {e_sql}")
        #      return None, None
        return None, None # Falha se faltar variável PG

    # Tentar conectar ao PostgreSQL
    try:
        conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            database=pg_database,
            user=pg_user,
            password=pg_password
        )
        conn.autocommit = True # Importante para DML simples no worker
        print("INFO [Worker DB]: Conexão PostgreSQL estabelecida.")
        return conn, "postgres"
    except PgError as e_pg:
        print(f"ERRO [Worker DB]: Falha ao conectar ao PostgreSQL: {e_pg}")
        print(f"  Detalhes: Host={pg_host}, Port={pg_port}, DB={pg_database}, User={pg_user}")
        return None, None
    except Exception as e:
        print(f"ERRO CRÍTICO [Worker DB]: Erro inesperado na conexão: {e}")
        return None, None

def close_connection_worker(conn_info):
    """Fecha a conexão com o banco de dados (VERSÃO WORKER)."""
    if conn_info is None or conn_info[0] is None:
        return
    conn, conn_type = conn_info
    try:
        conn.close()
        # print(f"DEBUG [Worker DB]: Conexão {conn_type} fechada.")
    except Exception as e:
        print(f"AVISO [Worker DB]: Erro ao fechar conexão {conn_type}: {e}")

# Exemplo de execute_query (COLE A SUA VERSÃO COMPLETA)
def execute_query(query, params=None, fetch_one=False, fetch_all=False, is_dml=False):
    """Executa uma query no banco de dados (VERSÃO WORKER)."""
    conn_info = get_db_connection_worker()
    if conn_info is None or conn_info[0] is None:
        print(f"ERRO [Execute Query]: Falha ao obter conexão com DB para query: {query[:50]}...")
        return None # Retorna None se não conseguiu conectar

    conn, conn_type = conn_info
    result = None
    cursor = None

    try:
        # Verificar se a conexão está fechada (para PostgreSQL)
        if conn_type == "postgres":
            if getattr(conn, 'closed', 0) != 0: # Verifica se conn.closed não é 0 (está fechada)
                print("AVISO [Execute Query]: Conexão PG estava fechada. Tentando reabrir...")
                close_connection_worker(conn_info) # Fecha formalmente se possível
                conn_info = get_db_connection_worker() # Tenta conectar novamente
                if conn_info is None or conn_info[0] is None:
                    print("ERRO [Execute Query]: Falha ao reabrir conexão.")
                    return None
                conn, conn_type = conn_info

        cursor = conn.cursor()

        # Adapta placeholders (%s vs ?) - essencial se usar SQLite como fallback
        adapted_query = query
        if params is not None:
            # Assume PostgreSQL como primário (usa %s), adapta se for SQLite
            if conn_type == "sqlite" and "%s" in query:
                adapted_query = query.replace("%s", "?")
            # Não precisa adaptar de ? para %s se PG é o padrão

            # print(f"DEBUG [Execute Query]: Executando: {adapted_query} com Params: {params}") # Log detalhado
            cursor.execute(adapted_query, params)
        else:
            # print(f"DEBUG [Execute Query]: Executando: {adapted_query}") # Log detalhado
            cursor.execute(adapted_query)

        # Commit é feito pelo autocommit=True na conexão ou manualmente se necessário
        # if is_dml and not conn.autocommit:
        #    conn.commit()

        if is_dml:
            result = cursor.rowcount # Número de linhas afetadas
        elif fetch_one:
            result = cursor.fetchone() # Retorna uma tupla ou None
        elif fetch_all:
            result = cursor.fetchall() # Retorna lista de tuplas ou lista vazia []

        # print(f"DEBUG [Execute Query]: Resultado: {result}") # Log detalhado

    except (PgError, sqlite3.Error, Exception) as e:
        error_type = type(e).__name__
        print(f"ERRO [Execute Query] ({error_type}): {e}")
        print(f"  Query: {query}")
        print(f"  Params: {params}")
        try:
            if conn and not conn.autocommit: conn.rollback() # Tenta rollback se não houver autocommit
        except: pass
        result = None # Retorna None em caso de erro

    finally:
        if cursor:
            try: cursor.close()
            except: pass
        # Fecha a conexão após cada query para evitar conexões ociosas longas no worker
        close_connection_worker(conn_info)

    return result


# NOVA Função para buscar TODAS as configs
def get_all_api_configs_worker():
    """Busca todas as configurações de API do banco (VERSÃO WORKER)."""
    print("INFO [Worker]: Buscando todas as configurações de API...")
    query = """
        SELECT id, name, app_id, app_secret, access_token, account_id, token_expires_at
        FROM api_config ORDER BY id -- Busca todas, não filtra por is_active
    """
    rows = execute_query(query, fetch_all=True)
    configs = []
    if rows:
        keys = ["id", "name", "app_id", "app_secret", "access_token", "account_id", "token_expires_at"]
        for row in rows:
            configs.append(dict(zip(keys, row)))
        print(f"INFO [Worker]: {len(configs)} configurações de API encontradas.")
    else:
        print("AVISO [Worker]: Nenhuma configuração de API encontrada no banco.")
    return configs

# init_facebook_api_worker MODIFICADA para receber a config
def init_facebook_api_worker(config):
    """Inicializa a API do Facebook para uma config específica (VERSÃO WORKER)."""
    if not config:
        print("ERRO [Worker Init FB]: Configuração inválida fornecida.")
        return None

    print(f"INFO [Worker Init FB]: Inicializando API para Conta ID {config.get('account_id')} (Nome: {config.get('name', 'N/A')})...")

    required_keys = ["app_id", "app_secret", "access_token", "account_id"]
    missing_keys = [key for key in required_keys if not config.get(key)]
    if missing_keys:
         print(f"ERRO [Worker Init FB]: Configuração ID {config.get('id')} incompleta. Faltam: {', '.join(missing_keys)}.")
         return None

    expires_date = config.get('token_expires_at')
    if isinstance(expires_date, date):
        if expires_date < date.today():
            print(f"ERRO [Worker Init FB]: Token para conta ID {config.get('id')} expirou em {expires_date.strftime('%d/%m/%Y')}.")
            return None # Retorna None se o token expirou
    # else: print(f"AVISO [Worker Init FB]: Data de expiração do token não definida para config ID {config.get('id')}.")

    try:
        # REMOVA a linha com clear_instance=True - NÃO É NECESSÁRIA/SUPORTADA
        # FacebookAdsApi.init(clear_instance=True) # <<< LINHA REMOVIDA

        # Inicializa diretamente com a config atual.
        # Esta chamada sobrescreverá a instância singleton anterior.
        FacebookAdsApi.init(
            app_id=config["app_id"],
            app_secret=config["app_secret"],
            access_token=config["access_token"],
            api_version='v20.0' # É uma boa prática especificar a versão
        )
        # Verifica a conexão para esta conta específica
        account_str_id = f'act_{config["account_id"]}'
        # Tenta fazer uma chamada leve para confirmar que as credenciais funcionam
        AdAccount(account_str_id).api_get(fields=['id'])
        print(f"INFO [Worker Init FB]: Conexão com conta {account_str_id} OK.")
        return config["account_id"] # Retorna o ID da conta se sucesso
    except Exception as e:
        # Log do erro real encontrado
        print(f"ERRO [Worker Init FB]: Falha ao inicializar/verificar API para conta {config.get('account_id')}: {e}")
        # Descomente a linha abaixo se precisar de mais detalhes para outros erros
        # print(f"Traceback: {traceback.format_exc()}")
        return None # Retorna None em caso de erro

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
    """Verifica e executa regras automáticas agendadas PARA TODAS AS CONTAS."""
    start_time = time.time()
    print(f"\n--- [WORKER START - Multi-Conta] {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')} ---")

    total_actions_executed = 0
    accounts_processed_count = 0
    accounts_failed_init_count = 0
    rules_checked_count = 0
    rules_executed_count = 0

    # 1. Buscar TODAS as configurações de API
    all_configs = get_all_api_configs_worker()
    if not all_configs:
        print("AVISO [Worker]: Nenhuma configuração de API encontrada para processar. Saindo.")
        return

    # 2. Buscar TODAS as regras automáticas ativas
    print("INFO [Worker]: Buscando regras automáticas ativas...")
    rule_query = """
        SELECT id, name, execution_interval_hours, last_automatic_run_at,
               is_composite, primary_metric, primary_operator, primary_value,
               secondary_metric, secondary_operator, secondary_value, join_operator,
               action_type, action_value
        FROM rules
        WHERE execution_mode = 'automatic' AND is_active = 1 ORDER BY id
    """
    rules_data = execute_query(rule_query, fetch_all=True)
    if rules_data is None:
        print("ERRO [Worker]: Falha ao buscar regras do banco de dados. Saindo.")
        return
    if not rules_data:
        print("INFO [Worker]: Nenhuma regra automática ativa encontrada para processar. Saindo.")
        return

    print(f"INFO [Worker]: {len(rules_data)} regras automáticas ativas encontradas.")
    # Mapeia nomes das colunas para facilitar acesso (IMPORTANTE: a ordem deve bater com o SELECT)
    rule_keys = ["id", "name", "execution_interval_hours", "last_automatic_run_at", "is_composite",
                 "primary_metric", "primary_operator", "primary_value", "secondary_metric",
                 "secondary_operator", "secondary_value", "join_operator", "action_type", "action_value"]
    all_rules = [dict(zip(rule_keys, rule_tuple)) for rule_tuple in rules_data]

    now_utc = datetime.now(timezone.utc)
    rules_due_to_run = [] # Lista para guardar regras que precisam rodar AGORA
    rules_to_update_timestamp = set() # Conjunto para guardar IDs das regras cujo timestamp deve ser atualizado no final

    # 3. PRIMEIRO LOOP: Verificar quais regras estão "due"
    print("INFO [Worker]: Verificando quais regras estão no horário de execução...")
    for rule in all_rules:
        rules_checked_count += 1
        rule_id = rule['id']
        interval_h = rule.get('execution_interval_hours')
        last_run_ts = rule.get('last_automatic_run_at')

        if not interval_h or interval_h <= 0:
            # print(f"DEBUG [Worker]: Regra ID {rule_id} ('{rule.get('name')}') com intervalo inválido ({interval_h}h). Pulando.")
            continue

        is_due = False
        if last_run_ts is None:
            is_due = True
            print(f"  -> Regra ID {rule_id} ('{rule.get('name')}'): Primeira execução automática.")
        else:
            # Garante que last_run_ts tenha timezone (assume UTC se não tiver)
            if isinstance(last_run_ts, datetime) and last_run_ts.tzinfo is None:
                last_run_ts = last_run_ts.replace(tzinfo=timezone.utc)
            elif not isinstance(last_run_ts, datetime):
                 print(f"  AVISO: 'last_automatic_run_at' para regra ID {rule_id} não é um datetime válido ({type(last_run_ts)}). Considerando como primeira execução.")
                 is_due = True # Trata como due se o timestamp for inválido

            if not is_due and isinstance(last_run_ts, datetime): # Só calcula next_due se não for a primeira/inválida
                try:
                    next_due_time = last_run_ts + timedelta(hours=interval_h)
                    # print(f"  DEBUG: Regra ID {rule_id} - Última: {last_run_ts.strftime('%Y-%m-%d %H:%M %Z')}, Próxima >= {next_due_time.strftime('%Y-%m-%d %H:%M %Z')}")
                    if now_utc >= next_due_time:
                        is_due = True
                        print(f"  -> Regra ID {rule_id} ('{rule.get('name')}'): Pronta para executar.")
                    # else: print(f"  -> Regra ID {rule_id}: Ainda não está na hora.")
                except Exception as e_delta:
                     print(f"  ERRO: Calculando next_due_time para regra ID {rule_id}: {e_delta}. Pulando regra.")
                     continue # Pula esta regra se houver erro no cálculo

        if is_due:
            rules_due_to_run.append(rule) # Adiciona a regra completa à lista das que devem rodar
            rules_to_update_timestamp.add(rule_id) # Marca para atualizar o timestamp DEPOIS

    if not rules_due_to_run:
        print("INFO [Worker]: Nenhuma regra automática no horário para executar neste ciclo.")
    else:
        print(f"INFO [Worker]: {len(rules_due_to_run)} regras prontas para serem aplicadas nas contas.")

        # 4. SEGUNDO LOOP: Iterar sobre as CONTAS
        for config in all_configs:
            config_id = config.get('id')
            account_id_str = config.get('account_id')
            config_name = config.get('name', f'Config ID {config_id}')
            print(f"\n===== Processando Conta: {config_name} (act_{account_id_str}) para regras prontas =====")
            accounts_processed_count += 1
            actions_this_account = 0

            # 4.1. Inicializar API para esta conta
            current_account_id = init_facebook_api_worker(config)
            if not current_account_id:
                print(f"AVISO [Worker]: Falha ao inicializar API para {config_name}. Pulando regras para esta conta.")
                accounts_failed_init_count += 1
                continue # Pula para a próxima conta

            # 4.2. Buscar campanhas desta conta
            campaigns_this_account = get_facebook_campaigns(current_account_id)
            if campaigns_this_account is None:
                print(f"ERRO [Worker]: Falha ao buscar campanhas para {config_name}. Pulando regras para esta conta.")
                continue
            campaigns_to_check = [c for c in campaigns_this_account if isinstance(c, dict) and c.get('effective_status') not in ['ARCHIVED', 'DELETED']]
            print(f"INFO [Worker]: {len(campaigns_to_check)} campanhas elegíveis encontradas para {config_name}.")

            if not campaigns_to_check:
                 print(f"INFO [Worker]: Nenhuma campanha elegível em {config_name} para aplicar regras.")
            else:
                # 4.3. Iterar sobre as REGRAS QUE ESTÃO "DUE"
                for rule in rules_due_to_run: # Usa a lista filtrada
                    rule_id = rule['id']
                    rule_name = rule['name']
                    print(f"  ---> Aplicando Regra ID {rule_id} ('{rule_name}') na Conta {config_name}")
                    rule['is_active'] = 1 # Garante que está ativa para simulação

                    # 4.4. Iterar sobre as CAMPANHAS desta conta
                    for campaign in campaigns_to_check:
                        campaign_id = campaign.get('id')
                        campaign_name = campaign.get('name', f"ID {campaign_id}")
                        # print(f"    -> Verificando Campanha ID {campaign_id} ('{campaign_name[:30]}...')") # Log muito verboso
                        try:
                            # Simula APENAS a regra atual na campanha atual
                            sim_results = simulate_rule_application(campaign, [rule])
                            # Verifica se a simulação retornou um resultado para ESTA regra
                            if sim_results and any(s.get('rule_id') == rule_id for s in sim_results):
                                print(f"      ✅ Condição ATENDIDA para Campanha ID {campaign_id} ('{campaign_name[:30]}...'). EXECUTANDO AÇÃO...")
                                rules_executed_count +=1 # Conta quantas vezes uma regra é ATIVADA
                                success_exec, msg_exec = execute_rule(campaign_id, rule_id)
                                if success_exec:
                                     actions_this_account += 1
                                     print(f"      ✅ Ação executada: {msg_exec}")
                                else:
                                     print(f"      ❌ Falha na execução: {msg_exec}")
                                # execute_rule já faz o log no banco
                            # else: print(f"    -> Condição NÃO atendida para Campanha ID {campaign_id}.") # Log muito verboso

                        except Exception as sim_exec_err:
                             print(f"    -> ERRO CRÍTICO sim/exec Regra ID {rule_id} na Campanha ID {campaign_id}: {sim_exec_err}")
                             log_rule_execution(rule_id, campaign_id, 'campaign', campaign_name, False, f"Worker sim/exec error: {str(sim_exec_err)[:150]}")

            print(f"===== Conta {config_name} processada. Ações executadas nesta conta: {actions_this_account} =====")
            total_actions_executed += actions_this_account

    # 5. TERCEIRO PASSO: Atualizar Timestamps DAS REGRAS QUE RODARAM (estavam "due")
    if rules_to_update_timestamp:
        print(f"\nINFO [Worker]: Atualizando 'last_automatic_run_at' para {len(rules_to_update_timestamp)} regras...")
        updated_count = 0
        for rule_id_to_update in rules_to_update_timestamp:
            # Usamos now_utc que foi pego no início do script para consistência
            update_result = execute_query(
                "UPDATE rules SET last_automatic_run_at = %s, updated_at = %s WHERE id = %s",
                (now_utc, now_utc, rule_id_to_update), is_dml=True
            )
            if update_result is not None and update_result > 0:
                # print(f"  -> Timestamp atualizado para regra ID {rule_id_to_update}.")
                updated_count += 1
            else:
                 print(f"  ERRO: Falha ao atualizar timestamp para regra ID {rule_id_to_update}.")
        print(f"INFO [Worker]: {updated_count} timestamps atualizados com sucesso.")
    else:
         print("INFO [Worker]: Nenhum timestamp de regra para atualizar.")


    # 6. Log Final
    end_time = time.time()
    duration = end_time - start_time
    print(f"\n--- [WORKER END - Multi-Conta] {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')} ---")
    print(f"Tempo total: {duration:.2f} seg.")
    print(f"Contas Verificadas: {accounts_processed_count} (Falha na inicialização: {accounts_failed_init_count})")
    print(f"Total de Regras Automáticas Ativas Verificadas: {rules_checked_count}")
    print(f"Total de Ativações de Regra (condição atendida): {rules_executed_count}")
    print(f"Total de Ações de API Executadas (todas as contas): {total_actions_executed}")


# --- Ponto de Entrada do Script ---
if __name__ == "__main__":
    # ... (verificação de variáveis de ambiente permanece a mesma) ...
    required_env_vars = ["PGHOST", "PGUSER", "PGPASSWORD", "PGPORT", "PGDATABASE"]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        print(f"ERRO FATAL [Worker]: Variáveis de ambiente faltando: {', '.join(missing_vars)}")
        sys.exit(1)

    print(f"INFO [Worker]: Iniciando execução do script {os.path.basename(__file__)}")
    run_automatic_rules() # Chama a função reestruturada
    print(f"INFO [Worker]: Script {os.path.basename(__file__)} concluído.")