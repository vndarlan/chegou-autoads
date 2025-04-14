# principal/tutorial.py
import streamlit as st

# --- Usar st.columns para criar layout centralizado ---
# Ajuste os números na lista para mudar a largura relativa da área central
left_space, content_area, right_space = st.columns([1, 2.5, 1]) # Mantenha ou ajuste os ratios

# Coloque TODO o conteúdo dentro da coluna central 'content_area'
with content_area:
    # --- Cabeçalho da Página ---
    st.markdown('<h1 style="text-align: center;">Tutoriais</h1>', unsafe_allow_html=True)
    st.markdown('<p style="text-align: center;"><strong>Guia prático: aprenda a usar a ferramenta passo a passo!</strong></p>', unsafe_allow_html=True)
    st.divider()

    # --- Módulo 1 ---
    st.subheader("Gerenciador de Anúncios: Introdução e Configuração Inicial")
    st.write("Comece por aqui!") # Descrição opcional do módulo

    # Vídeo 1 do Módulo 1
    with st.container(border=True): # 'border=True' adiciona uma borda sutil para agrupar visualmente
        st.markdown("##### **Vídeo 1:** Como conectar sua conta do Facebook") # Título do vídeo com ênfase na sequência
        st.video("https://youtu.be/pSJM8rNgIg0") # URL Placeholder
        st.write("Criar aplicativo: https://developers.facebook.com/async/registration/dialog/?src=default") # Descrição do vídeo
        st.write("Permições para o token: read_insights, ads_management, ads_read, business_management") # Descrição do vídeo

    # Espaçador visual entre os vídeos do mesmo módulo (opcional)
    st.write("") # Adiciona um pequeno espaço vertical

    # Vídeo 2 do Módulo 1
    with st.container(border=True):
        st.markdown("##### **Vídeo 2:** EM BREVE") # Título do vídeo com ênfase na sequência
        st.video("https://www.youtube.com/watch") # URL Placeholder

    st.divider() # Separador entre Módulo 1 e Módulo 2

    # --- Módulo 2 ---
    st.subheader("Módulo 2: EM BREVE")

    # Vídeo 1 do Módulo 2
    with st.container(border=True):
        st.markdown("##### **Vídeo 1:** EM BREVE")
        st.write("EM BREVE")
        st.video("https://www.youtube.com/watch") # URL Placeholder

    st.divider() # Adicione um divisor final ou para separar do próximo módulo

    # --- Você pode adicionar mais Módulos seguindo o padrão acima ---
    # st.subheader("Módulo 3: Tópicos Avançados")
    # with st.container(border=True):
    #     st.markdown("##### **Vídeo 1:** ...")
    #     st.write("...")
    #     st.video("URL_VIDEO_AQUI")
    # st.divider()


# As colunas 'left_space' e 'right_space' permanecem vazias.