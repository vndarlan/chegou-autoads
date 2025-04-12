# principal/tutorial.py
import streamlit as st

# --- Remover QUALQUER CSS anterior para centralizar ---
# st.markdown("""<style>...</style>""", unsafe_allow_html=True) # Remova ou comente isso

# --- Usar st.columns para criar layout centralizado ---
# Ajuste os números na lista para mudar a largura relativa da área central
# Ex: [1, 2, 1] -> coluna central tem o dobro da largura das laterais
# Ex: [1, 3, 1] -> coluna central tem o triplo da largura das laterais
left_space, content_area, right_space = st.columns([1, 2.5, 1]) # Experimente com os ratios

# Coloque TODO o conteúdo original dentro da coluna central 'content_area'
with content_area:
    # Page header
    # Usar st.container() aqui pode ajudar a agrupar e alinhar se necessário
    with st.container():
        # Você ainda pode usar markdown para títulos, mas talvez centralizar texto seja mais fácil agora
        st.markdown('<h1 style="text-align: center;">Tutoriais</h1>', unsafe_allow_html=True)
        st.markdown('<p style="text-align: center;"><strong>Guia prático: aprenda a usar a ferramenta passo a passo!</strong></p>', unsafe_allow_html=True)
        st.divider() # Substitui st.markdown("---") para consistência

    # First video (featured)
    # Novamente, agrupar com st.container pode ser útil
    with st.container():
        st.markdown('<div style="text-align: center;"><strong>Novelties Sem Complicação!</strong></div>', unsafe_allow_html=True)
        st.markdown('<div style="text-align: center;">Aprenda a ativar e usar a automação</div>', unsafe_allow_html=True)
        st.video("https://www.youtube.com/watch?v=_AgxTh3ddyM")
        st.divider()

    # Adicione mais conteúdo aqui DENTRO do 'with content_area:'
    # Exemplo: Outro vídeo
    # with st.container():
    #     st.markdown('<div style="text-align: center;"><strong>Outro Tutorial</strong></div>', unsafe_allow_html=True)
    #     st.markdown('<div style="text-align: center;">Descrição do outro tutorial</div>', unsafe_allow_html=True)
    #     st.video("URL_DO_SEU_VIDEO_AQUI")
    #     st.divider()


# As colunas 'left_space' e 'right_space' permanecem vazias, criando o efeito de centralização.
# Não coloque nada fora do bloco 'with content_area:' se quiser que fique centralizado.