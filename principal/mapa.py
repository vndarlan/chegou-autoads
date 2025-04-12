# principal/home.py
import streamlit as st
import folium
from streamlit_folium import st_folium
import os
import pandas as pd
import requests # To download the GeoJSON if needed

# --- Configuration ---
# Define country lists directly in the code
COUNTRY_DATA = {
    "active": ["Chile", "Mexico", "Colombia"],
    "past": ["Brazil", "Panama", "Ecuador"],      # Will be RED
    "expanding": ["Italy", "Spain"]              # Will be ORANGE
}

# Mapping for name variations (Original in COUNTRY_DATA -> English for GeoJSON matching)
NAME_MAP = {
    "México": "Mexico", "Colombia": "Colombia", "Chile": "Chile",
    "Brasil": "Brazil", "Panamá": "Panama", "Equador": "Ecuador",
    "Itália": "Italy", "Espanha": "Spain"
}

# Approximate coordinates for marker placement [latitude, longitude]
COUNTRY_COORDINATES = {
    "Chile": [-31.7, -71.0], "Mexico": [23.6, -102.5], "Colombia": [4.6, -74.0],
    "Brazil": [-14.2, -51.9], "Panama": [8.5, -80.7], "Ecuador": [-1.8, -78.1],
    "Italy": [41.9, 12.5], "Spain": [40.4, -3.7],
}

# Define colors for each status (UPDATED)
STATUS_COLORS = { # Folium color names for icons
    "active": "green",
    "past": "red",         # Changed to red
    "expanding": "orange", # Changed to orange
    "default": "lightgray" # Map fill default
}
STATUS_COLORS_HEX = { # Hex codes for HTML legend/lists and map fill (UPDATED)
    "active": "#4CAF50",    # Green
    "past": "#FF0000",      # Red
    "expanding": "#FFA500", # Orange
    "default": "#D3D3D3"    # Light Gray
}

# GeoJSON configuration
GEOJSON_URL = "https://raw.githubusercontent.com/python-visualization/folium/master/examples/data/world-countries.json"
GEOJSON_LOCAL_PATH = "assets/world-countries.json"

# --- Helper Functions ---

def download_geojson(url, path):
    """Downloads the GeoJSON file if it doesn't exist locally."""
    if not os.path.exists(path):
        st.warning(f"Arquivo GeoJSON não encontrado em {path}. Baixando de {url}...")
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            response = requests.get(url)
            response.raise_for_status()
            with open(path, 'wb') as f:
                f.write(response.content)
            st.success(f"GeoJSON baixado e salvo em {path}")
        except requests.exceptions.RequestException as e:
            st.error(f"Falha ao baixar o arquivo GeoJSON: {e}")
        except OSError as e:
             st.error(f"Erro ao criar diretório ou salvar arquivo GeoJSON: {e}")

    if not os.path.exists(path):
         st.error(f"Não foi possível obter ou salvar o arquivo GeoJSON em {path}.")
         return None
    return path

def create_country_status_df(country_data_dict, name_mapping):
    """Creates a DataFrame mapping country names to statuses, using the provided name_mapping."""
    data = []
    for status, countries in country_data_dict.items():
        for country in countries:
            normalized_name = name_mapping.get(country, country)
            data.append({"country": normalized_name, "status": status})
    if not data:
        return pd.DataFrame(columns=['country', 'status'])
    return pd.DataFrame(data)

# --- Streamlit Page ---

st.title("🗺️ Mapa de Atuação")
st.markdown("Visualize os países onde o Grupo Chegou opera, operou ou está expandindo.")

# --- Load Data & GeoJSON ---
local_geojson_path = download_geojson(GEOJSON_URL, GEOJSON_LOCAL_PATH)

# Create columns: 4 parts for the map, 1 part for the legend (adjust ratio if needed)
map_col, legend_col = st.columns([4, 1])

with map_col:
    if local_geojson_path:
        # Prepare data for coloring countries
        country_status_df = create_country_status_df(COUNTRY_DATA, NAME_MAP)

        # Create the base map
        m = folium.Map(location=[20, -30], zoom_start=2.5, tiles="CartoDB positron", prefer_canvas=True) # Added prefer_canvas

        if not country_status_df.empty:
            # Dict for country fill colors
            country_to_fill_color = {
                row['country']: STATUS_COLORS_HEX[row['status']]
                for index, row in country_status_df.iterrows()
            }

            # --- Add GeoJson Layer for Country Coloring ---
            style_function = lambda x: {
                'fillColor': country_to_fill_color.get(x['properties']['name'], STATUS_COLORS_HEX['default']),
                'color': 'black', 'weight': 0.5,
                'fillOpacity': 0.65 if x['properties']['name'] in country_to_fill_color else 0.2 # Slightly more opaque fill
            }
            tooltip_geojson = folium.features.GeoJsonTooltip(
                fields=['name'], aliases=['País:'], localize=True, sticky=False,
                 style=("background-color: white; color: black; font-family: arial; font-size: 12px; padding: 5px;")
            )
            folium.GeoJson(
                local_geojson_path, name='Status dos Países', style_function=style_function,
                tooltip=tooltip_geojson,
                highlight_function=lambda x: {'weight': 1, 'fillOpacity': 0.85 if x['properties']['name'] in country_to_fill_color else 0.4},
            ).add_to(m)

            # --- Add Markers with Icons ---
            marker_group = folium.FeatureGroup(name="Ícones de Status")
            name_map_reverse = {v: k for k, v in NAME_MAP.items()}

            for status, countries in COUNTRY_DATA.items():
                 icon_color = STATUS_COLORS[status]
                 icon_symbol = 'flag'
                 status_description = {
                     "active": "Atuando Atualmente", "past": "Já Atuamos (Não Ativo)", "expanding": "Em Expansão / Validação"
                 }.get(status, status.capitalize())

                 for country_orig in countries:
                     country_norm = NAME_MAP.get(country_orig, country_orig)
                     if country_norm in COUNTRY_COORDINATES:
                         coords = COUNTRY_COORDINATES[country_norm]
                         display_name = name_map_reverse.get(country_norm, country_norm)
                         folium.Marker(
                             location=coords, tooltip=f"{display_name} - {status_description}",
                             icon=folium.Icon(color=icon_color, icon=icon_symbol, prefix='fa'),
                         ).add_to(marker_group)
                     else:
                         # Display warning in the main area, not just console
                         st.warning(f"Coordenadas não encontradas para '{country_orig}' (Normalizado: '{country_norm}'). Ícone não adicionado.")

            marker_group.add_to(m)

            # Display the map in the map column
            st_folium(m, use_container_width=True, height=650, returned_objects=[]) # Adjusted height slightly

        else:
            st.warning("Nenhum dado de país definido no código para exibir no mapa.")
            m = folium.Map(location=[20, -30], zoom_start=2, tiles="CartoDB positron")
            st_folium(m, use_container_width=True, height=650, returned_objects=[])

    else:
        st.error("Não foi possível carregar o mapa devido à falta do arquivo GeoJSON.")
        st.info("Verifique a conexão com a internet ou se o arquivo `assets/world-countries.json` existe.")

with legend_col:
    # --- Custom Legend in the legend column ---
    st.markdown("##### Legenda") # Smaller heading
    icon_html = f"<i class='fa fa-flag' style='color: black; font-size: 1.1em; vertical-align: middle;'></i>" # Specific icon used

    # Combine legend items for potentially tighter spacing
    legend_html = f"""
    <div style="line-height: 1.6;">
    <span style='color:{STATUS_COLORS_HEX['active']}; font-size: 1.3em; vertical-align: middle;'>■</span> <span style='vertical-align: middle; font-size: 0.9em;'>Atuando</span><br>
    <span style='color:{STATUS_COLORS_HEX['expanding']}; font-size: 1.3em; vertical-align: middle;'>■</span> <span style='vertical-align: middle; font-size: 0.9em;'>Em Expansão / Validação</span><br>
    <span style='color:{STATUS_COLORS_HEX['past']}; font-size: 1.3em; vertical-align: middle;'>■</span> <span style='vertical-align: middle; font-size: 0.9em;'>Já Atuamos (Não Ativo)</span><br>
    <span style='color:{STATUS_COLORS_HEX['default']}; font-size: 1.3em; vertical-align: middle;'>■</span> <span style='vertical-align: middle; font-size: 0.9em;'>Outros Países</span><br>
    {icon_html} <span style='vertical-align: middle; font-size: 0.9em;'>Ícone de Presença</span>
    </div>
    """
    st.markdown(legend_html, unsafe_allow_html=True)


# --- Display Current Lists (Below Map/Legend) ---
st.divider()

col1, col2, col3 = st.columns(3) # Keep lists in 3 columns

with col1:
    st.markdown(f"**<span style='color:{STATUS_COLORS_HEX['active']};'>Atuando</span>**", unsafe_allow_html=True)
    active_countries = COUNTRY_DATA.get("active", [])
    if active_countries: st.markdown("\n".join([f"- {country}" for country in active_countries]))
    else: st.write("Nenhum")

with col2:
    # Use the correct status key and color for the title
    st.markdown(f"**<span style='color:{STATUS_COLORS_HEX['expanding']};'>Em Expansão</span>**", unsafe_allow_html=True)
    expanding_countries = COUNTRY_DATA.get("expanding", [])
    if expanding_countries: st.markdown("\n".join([f"- {country}" for country in expanding_countries]))
    else: st.write("Nenhum")

with col3:
     # Use the correct status key and color for the title
    st.markdown(f"**<span style='color:{STATUS_COLORS_HEX['past']};'>Já Atuamos</span>**", unsafe_allow_html=True)
    past_countries = COUNTRY_DATA.get("past", [])
    if past_countries: st.markdown("\n".join([f"- {country}" for country in past_countries]))
    else: st.write("Nenhum")

st.markdown("""
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css">
""", unsafe_allow_html=True)