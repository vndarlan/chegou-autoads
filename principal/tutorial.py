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
    st.subheader("Módulo 1: Introdução e Configuração Inicial")
    st.write("Comece por aqui! Aprenda os conceitos básicos e como ativar a principal automação.") # Descrição opcional do módulo

    # Vídeo 1 do Módulo 1
    with st.container(border=True): # 'border=True' adiciona uma borda sutil para agrupar visualmente
        st.markdown("##### **Vídeo 1:** Visão Geral da Plataforma") # Título do vídeo com ênfase na sequência
        st.write("Explore a interface principal e entenda as funcionalidades chave disponíveis.") # Descrição do vídeo
        st.video("https://www.youtube.com/watch?v=_AgxTh3ddyM") # URL Placeholder

    # Espaçador visual entre os vídeos do mesmo módulo (opcional)
    st.write("") # Adiciona um pequeno espaço vertical

    # Vídeo 2 do Módulo 1
    with st.container(border=True):
        st.markdown("##### **Vídeo 2:** Ativando a Automação Novelties") # Título do vídeo com ênfase na sequência
        st.write("Siga o passo a passo detalhado para configurar e colocar a automação 'Novelties' para rodar.")
        st.video("https://www.youtube.com/watch?v=_AgxTh3ddyM") # URL Placeholder

    st.divider() # Separador entre Módulo 1 e Módulo 2

    # --- Módulo 2 ---
    st.subheader("Módulo 2: Analisando Resultados")
    st.write("Após ativar, aprenda como acompanhar e interpretar os resultados.")

    # Vídeo 1 do Módulo 2
    with st.container(border=True):
        st.markdown("##### **Vídeo 1:** Entendendo o Dashboard Principal")
        st.write("Descubra como ler os gráficos e métricas apresentados no seu dashboard.")
        st.video("https://www.youtube.com/watch?v=_AgxTh3ddyM") # URL Placeholder

    st.divider() # Adicione um divisor final ou para separar do próximo módulo

    # --- Você pode adicionar mais Módulos seguindo o padrão acima ---
    # st.subheader("Módulo 3: Tópicos Avançados")
    # with st.container(border=True):
    #     st.markdown("##### **Vídeo 1:** ...")
    #     st.write("...")
    #     st.video("URL_VIDEO_AQUI")
    # st.divider()


# As colunas 'left_space' e 'right_space' permanecem vazias.