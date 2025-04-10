import streamlit as st
from streamlit.runtime.scriptrunner import RerunException, RerunData
import os # Mantenha o import os

# Função interna para forçar rerun (substitui st.experimental_rerun())
def force_rerun():
    raise RerunException(RerunData(None))

# Dicionário de usuários (AVISO: NÃO use em produção sem hashing de senhas!)
USERS = {
    "adminoperacional@grupochegou.com": {"password": "admgcopera2025", "cargo": "Administrador"},
    "operacional@grupochegou.com":  {"password": "gcopera2025",  "cargo": "Usuário"},
}
# --- AVISO DE SEGURANÇA ---
st.sidebar.warning("⚠️ **Atenção:** O sistema de login atual NÃO é seguro para produção. As senhas estão visíveis no código. Considere usar bibliotecas como `streamlit-authenticator` para um login seguro.")
# --- FIM AVISO ---


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
    """Exibe um botão de logout na sidebar."""
    st.sidebar.write(f"Logado como: {st.session_state.get('user_email', 'Usuário')}") # Mostra email logado
    if st.sidebar.button("Sair", key="logout_button"):
        # Limpa o estado da sessão
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        # Garante que logged_in seja False após limpar tudo
        st.session_state["logged_in"] = False
        st.session_state["cargo"] = None
        force_rerun()

def main():
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
        /* width: 280px !important; /* Exemplo: Fixar largura da sidebar */
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

        # Define páginas de acordo com o cargo (exemplo, ajuste conforme necessário)
        # No seu código original, ambos tinham as mesmas páginas
        if st.session_state["cargo"] == "Administrador":
            pages = {
                "Principal": [
                    st.Page("principal/home.py", title="Home", icon=":material/home:", default=True),
                    st.Page("principal/tutorial.py", title="Tutoriais", icon=":material/video_library:"),
                ],
                "Facebook Ads": [
                    st.Page("facebook/gerenciador.py",   title="Gerenciador",   icon=":material/manage_accounts:"), # Ícone diferente
                    st.Page("facebook/subir_campanha.py",    title="Subir Campanha",    icon=":material/upload_file:"), # Ícone diferente
                    st.Page("facebook/dashboard.py", title="Dashboard", icon=":material/dashboard:"), # Ícone diferente
                    # st.Page("facebook/configuracoes.py", title="Configurações", icon=":material/settings:"), # Ícone diferente - REMOVIDO? Gerenciador tem config
                ],
                # Adicione mais seções/páginas aqui se necessário
            }
        else: # Usuário comum (pode ter menos acesso)
             pages = {
                "Principal": [
                    st.Page("principal/home.py", title="Home", icon=":material/home:", default=True),
                    st.Page("principal/tutorial.py", title="Tutoriais", icon=":material/video_library:"),
                ],
                "Facebook Ads": [
                    st.Page("facebook/gerenciador.py",   title="Gerenciador",   icon=":material/manage_accounts:"),
                    st.Page("facebook/subir_campanha.py",    title="Subir Campanha",    icon=":material/upload_file:"),
                    st.Page("facebook/dashboard.py", title="Dashboard", icon=":material/dashboard:"),
                     # st.Page("facebook/configuracoes.py", title="Configurações", icon=":material/settings:"), # REMOVIDO?
                ],
            }

        # Cria a barra de navegação com as páginas definidas
        pg = st.navigation(pages, position="sidebar")
        # Exibe botão de logout
        show_logout_button()
        # Executa a página selecionada pelo usuário
        pg.run()

if __name__ == "__main__":
    # Define configuração da página AQUI, no script principal
    st.set_page_config(
        page_title="GC Operacional",
        page_icon="📊", # Ou um emoji/URL de sua preferência
        layout="centered", # Ou "wide" se preferir largura total por padrão
        initial_sidebar_state="expanded" # Ou "collapsed"
    )
    main()