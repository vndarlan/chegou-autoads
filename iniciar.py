import streamlit as st
from streamlit.runtime.scriptrunner import RerunException, RerunData

# Configuração global da página
st.set_page_config( 
    page_title="Chegou AutoADS", 
    page_icon="assets/favicon.png",
    layout="wide"  # Adicione esta linha para layout amplo
)

# Função interna para forçar rerun (substitui st.experimental_rerun())
def force_rerun():
    raise RerunException(RerunData(None))

# Dicionário de usuários (NÃO use em produção sem hashing de senhas)
USERS = {
    "adminautoads@grupochegou.com": {"password": "admgcads2025", "cargo": "Administrador"},
    "autoads@grupochegou.com":  {"password": "gcads2025",  "cargo": "Usuário"},
}

def login_page():
    """Página de Login."""
    st.title("Chegou AutoADS")
    st.subheader("Faça seu login")

    email = st.text_input("Email")
    password = st.text_input("Senha", type="password")

    if st.button("Entrar"):
        if email in USERS and USERS[email]["password"] == password:
            st.session_state["logged_in"] = True
            st.session_state["cargo"] = USERS[email]["cargo"]
            # Em vez de st.experimental_rerun(), usamos force_rerun():
            force_rerun()
        else:
            st.error("Credenciais inválidas. Tente novamente.")

def show_logout_button():
    """Exibe um botão de logout na sidebar."""
    if st.sidebar.button("Sair", key="logout_button"):
        st.session_state["logged_in"] = False
        st.session_state["cargo"] = None
        force_rerun()

def main():
    # Inicializa variáveis de sessão
    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False
    if "cargo" not in st.session_state:
        st.session_state["cargo"] = None

    # Adiciona CSS personalizado para a borda direita
    st.markdown("""
    <style>
    section[data-testid="stSidebar"] {
        border-right: 1px solid #e0e0e0;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Se NÃO estiver logado, exibe apenas a página de login
    if not st.session_state["logged_in"]:     
        pages = [st.Page(login_page, title="Login", icon=":material/lock:")]
        pg = st.navigation(pages, position="sidebar", expanded=False)
        pg.run()
    else:     
        # Define páginas de acordo com o cargo
        if st.session_state["cargo"] == "Administrador":
            pages = {
                "Principal": [
                    st.Page("principal/mapa.py", title="Mapa de Atuação", icon=":material/map:", default=True),
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
                    st.Page("principal/mapa.py", title="Mapa de Atuação", icon=":material/map:", default=True),
                    st.Page("principal/tutorial.py", title="Tutoriais", icon=":material/video_library:"),
                ],
                "Facebook Ads": [
                    st.Page("facebook/gerenciador.py",   title="Gerenciador",   icon=":material/manage_accounts:"),
                    st.Page("facebook/subir_campanha.py",    title="Subir Campanha",    icon=":material/upload_file:"),
                    st.Page("facebook/dashboard.py", title="Dashboard", icon=":material/dashboard:"),
                ],
            }

        # Cria a barra de navegação
        pg = st.navigation(pages, position="sidebar", expanded=False)
        # Exibe botão de logout
        show_logout_button()
        # Executa a página selecionada
        pg.run()

if __name__ == "__main__":
    main()