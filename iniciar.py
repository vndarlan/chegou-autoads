import streamlit as st
from streamlit.runtime.scriptrunner import RerunException, RerunData
import os

# --- Configuração da Página ---
# !!! MOVIDO PARA DENTRO DO if __name__ == "__main__": !!!
# st.set_page_config(...) DEVE ESTAR LÁ

# Função interna para forçar rerun
def force_rerun():
    raise RerunException(RerunData(None))

# Dicionário de usuários (AVISO: NÃO use em produção sem hashing de senhas!)
USERS = {
    "adminoperacional@grupochegou.com": {"password": "admgcopera2025", "cargo": "Administrador"},
    "operacional@grupochegou.com":  {"password": "gcopera2025",  "cargo": "Usuário"},
}
# --- AVISO DE SEGURANÇA ---
# !!! MOVIDO PARA DENTRO DA FUNÇÃO main() !!!
# st.sidebar.warning(...) ESTARÁ LÁ


def login_page():
    """Página de Login."""
    st.title("GC Operacional")
    st.subheader("Faça seu login")

    email = st.text_input("Email")
    password = st.text_input("Senha", type="password")

    if st.button("Entrar"):
        # --- AVISO: Comparação de senha insegura ---
        if email in USERS and USERS[email]["password"] == password:
            st.session_state["logged_in"] = True
            st.session_state["cargo"] = USERS[email]["cargo"]
            st.session_state["user_email"] = email # Opcional: guardar email
            force_rerun()
        else:
            st.error("Credenciais inválidas. Tente novamente.")

def show_logout_button():
    """Exibe informações do usuário e botão de logout na sidebar."""
    # Mostra email logado se disponível
    if "user_email" in st.session_state:
        st.sidebar.write(f"Logado como: {st.session_state['user_email']}")
    else:
         st.sidebar.write(f"Logado como: {st.session_state.get('cargo', 'Usuário')}") # Fallback para cargo

    # Botão Sair
    if st.sidebar.button("Sair", key="logout_button"):
        # Limpa o estado da sessão de forma segura
        keys_to_keep = [] # Adicione chaves que você queira manter, se houver
        for key in list(st.session_state.keys()):
            if key not in keys_to_keep:
                del st.session_state[key]
        # Garante que logged_in seja False após limpar tudo
        st.session_state["logged_in"] = False
        st.session_state["cargo"] = None
        force_rerun()

def main():
    """Função principal que controla a lógica da aplicação."""

    # --- AVISO DE SEGURANÇA ---
    # Movido para cá, após st.set_page_config ter sido chamado
    st.sidebar.warning("⚠️ **Atenção:** O sistema de login atual NÃO é seguro para produção. As senhas estão visíveis no código.")
    # --- FIM AVISO ---

    # Inicializa variáveis de sessão se não existirem
    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False
    if "cargo" not in st.session_state:
        st.session_state["cargo"] = None

    # Cria pastas necessárias para módulos Python (se não existirem)
    # REMOVIDO: Criação da pasta 'data'
    if not os.path.exists("facebook"):
        os.makedirs("facebook")
    if not os.path.exists("principal"):
        os.makedirs("principal")

    # Adiciona CSS personalizado para a borda direita da sidebar
    st.markdown("""
    <style>
    section[data-testid="stSidebar"] {
        border-right: 1px solid #e0e0e0; /* Cinza claro */
    }
    /* Ajuste opcional no padding do container principal */
    .main .block-container {
        padding-top: 2rem;
        padding-left: 2rem;
        padding-right: 2rem;
    }
    </style>
    """, unsafe_allow_html=True)

    # Lógica de navegação baseada no login
    if not st.session_state["logged_in"]:
        st.sidebar.header("ChegouOperation")
        st.sidebar.markdown("---")
        # Página de Login como única opção
        pages = [st.Page(login_page, title="Login", icon=":material/lock:", default=True)]
        pg = st.navigation(pages, position="sidebar")
        pg.run()
    else:
        # Usuário Logado
        st.sidebar.header("ChegouOperation")
        st.sidebar.markdown("---")

        # Define páginas de acordo com o cargo (ajuste conforme necessário)
        if st.session_state["cargo"] == "Administrador":
            pages = {
                "Principal": [
                    st.Page("principal/home.py", title="Home", icon=":material/home:", default=True),
                    st.Page("principal/tutorial.py", title="Tutoriais", icon=":material/video_library:"),
                ],
                "Facebook Ads": [
                    st.Page("facebook/gerenciador.py",   title="Gerenciador",   icon=":material/manage_accounts:"),
                    st.Page("facebook/subir_campanha.py",    title="Subir Campanha",    icon=":material/upload_file:"),
                    st.Page("facebook/dashboard.py", title="Dashboard", icon=":material/dashboard:"),
                ],
            }
        else: # Usuário comum
             pages = {
                "Principal": [
                    st.Page("principal/home.py", title="Home", icon=":material/home:", default=True),
                    st.Page("principal/tutorial.py", title="Tutoriais", icon=":material/video_library:"),
                ],
                "Facebook Ads": [
                    st.Page("facebook/gerenciador.py",   title="Gerenciador",   icon=":material/manage_accounts:"),
                    st.Page("facebook/subir_campanha.py",    title="Subir Campanha",    icon=":material/upload_file:"),
                    st.Page("facebook/dashboard.py", title="Dashboard", icon=":material/dashboard:"),
                ],
            }

        # Cria a barra de navegação com as páginas definidas
        pg = st.navigation(pages, position="sidebar")
        # Exibe botão de logout
        show_logout_button()
        # Executa a página selecionada pelo usuário
        pg.run()

# --- Ponto de Entrada Principal ---
if __name__ == "__main__":
    # --- st.set_page_config() COMO PRIMEIRO COMANDO STREAMLIT ---
    st.set_page_config(
        page_title="GC Operacional",
        page_icon="📊",
        layout="centered", # Mantido como 'centered' conforme seu setup original
        initial_sidebar_state="expanded"
    )
    # Agora chama a função principal que contém o resto da lógica e o warning
    main()