import pandas as pd
import numpy as np
import geopandas as gpd
import streamlit as st
import os
from scipy.spatial.distance import cdist
import folium
from streamlit_folium import st_folium

from src.utils import find_project_root


# CONSTANTS
# TODO: Move this to a configuration.py file
TABLE = 'acs_5yr_place_features_v1.parquet'
DATA_DIR = find_project_root() / "data" / "intermediate" / "features"
PARQUET_PATH = DATA_DIR / TABLE

# Define Metric Groups for Tab: Descriptive Analysis and Tab: Comparison
METRIC_GROUPS = {
    "ACS Base": ["pop_2024", "households_2024"],
    "ACS Income": ["median_income_2024", "median_home_value_2024"],
    "Property": ["unq_clips", "unq_addr_count", "condo_address_counts", "median_assessed_value", "median_tax_amount"],
    "Parcel": ["unq_parcel_count", "median_parcel_area_sq_mtr", "parcel_density"],
    "Growth": ["unq_growth_clips", "growth_clip_share", "business_count"]
}

@st.cache_data
def get_master_data():
    # 1. Check if the local Geoparquet exists
    if os.path.exists(PARQUET_PATH):
        #st.info("Loading data from local Geoparquet...")
        gdf = gpd.read_parquet(PARQUET_PATH)

        # Check if percentiles exist; if don't then add
        pct_cols = ['f{m}_pct' for group in METRIC_GROUPS.values() for m in group]
        if not all(col in gdf.columns for col in pct_cols):
            # st.info("Calculating national percentiles and updating local cache...")
            for group, metrics in METRIC_GROUPS.items():
                for m in metrics:
                    # rank True for 0-100 scale
                    gdf[f'{m}_pct'] = round(gdf[m].rank(pct=True)*100,2)
            gdf.to_parquet(PARQUET_PATH)
        return gdf
    else:
        st.error("Parquet file not found!")
        st.stop()

# Execute the cached loader
gdf = get_master_data()

# Main similarity engine
def run_similarity_logic(gdf, ref_geoids, target_states, pop_min, weights):
    """
    Computes similarity scores using DYNAMIC local percentiles based on user filters.
    """
    # Define the universe for which percentiles should be calculated
    # This is our specific sample for this calculation run
    universe = gdf[
        (gdf['state_name'].isin(target_states)) & 
        (gdf['pop_2024'] >= pop_min)
    ].copy()

    # universe without reference vectors
    
    if universe.empty:
        return universe

    # 2. Dynamic calculation of percentiles
    # Percentiles are calculated only in the subset
    # This is the main component of the engine and is calculated only for the session
    # local_pct_cols disappears once the session is refreshed
    local_pct_cols = []
    for group, metrics in METRIC_GROUPS.items():
        for m in metrics:
            local_col = f"{m}_local_pct"
            # We use rank(pct=True) to get a 0.0 to 1.0 range
            universe[local_col] = universe[m].rank(pct=True)
            local_pct_cols.append(local_col)

    # 3. CAPTURE THE LOCAL TARGET VECTOR
    # Look up how our reference cities rank WITHIN this new local context
    ref_mask = universe['geoidfq'].isin(ref_geoids)
    
    # Check if any reference cities actually exist in the current search universe
    if not ref_mask.any():
        st.warning("Note: Reference cities are outside the selected Search Universe. "
                   "Their scores are being benchmarked against the local market's distribution.")
        # Fallback: Get their values from the master GDF and project them into the local percentile distribution
        ref_df = gdf[gdf['geoidfq'].isin(ref_geoids)]
        target_v = []
        for m in [m for group in METRIC_GROUPS.values() for m in group]:
            # This 'projects' the reference raw value into the universe distribution
            raw_vals = ref_df[m].mean()
            local_pct = (universe[m] <= raw_vals).mean() # Percentile rank in current universe
            target_v.append(local_pct)
        target_v = np.array(target_v).reshape(1, -1)
    else:
        # Standard: Reference cities are part of the subset, just average their local ranks
        target_v = universe[ref_mask][local_pct_cols].mean().values.reshape(1, -1)

    # 4. COMPUTE WEIGHTED SIMILARITY
    # Pre-calculate the total weights for normalization
    total_weight = sum(weights.values())
    
    # We'll calculate the Euclidean distance for each group separately to apply weights
    weighted_distances_sq = 0
    
    for group, metrics in METRIC_GROUPS.items():
        group_cols = [f"{m}_local_pct" for m in metrics]
        group_weight = weights.get(group, 0.5)
        
        # Pick relevant columns for one metric group (e.g. ACS Basic) at a time
        group_target = target_v[:, [local_pct_cols.index(c) for c in group_cols]]
        # For this metric group, create matrix with geographies (rows) * relevant columns (columns)
        group_candidates = universe[group_cols].fillna(0).values
        
        # Calculate squared Euclidean distance within this metric group
        # group_target has shape [1*number of columns] - and group_candidates has shape [places*number of columns]
        # Docs: https://docs.scipy.org/doc/scipy/reference/generated/scipy.spatial.distance.cdist.html
        # Distance metric has a bunch of options. We've used squared euclidean to prioritize balanced candidates
        # Square the distance and taking square roots penalizes outliers (same concept as standard deviation)
        # TODO: https://stats.stackexchange.com/questions/118/why-square-the-difference-instead-of-taking-the-absolute-value-in-standard-devia
        # We had to use flatten because dists returns list of lists 
        dists = cdist(group_candidates, group_target, metric='sqeuclidean').flatten()
        
        # Accumulate the weighted squared distance
        weighted_distances_sq += dists * group_weight
        
        # Store individual group similarity for the tooltip/table (0-100 scale)
        # Max possible sq_dist per group is (number of metrics * 1.0^2)
        max_group_dist = np.sqrt(len(group_cols))
        universe[f"{group.lower().replace(' ', '_')}_sim"] = (1 - np.sqrt(dists) / max_group_dist) * 100

    # 5. Final Overall Score
    final_dist = np.sqrt(weighted_distances_sq / total_weight)
    universe['overall_similarity'] = (1 - final_dist) * 100
    
    # 6. Rank and Clean Up (Excluding References)
    # We remove the "seeds" before assigning ranks so the best look-alike is #1
    candidates_only = universe[~universe['geoidfq'].isin(ref_geoids)].copy()
    
    candidates_only = candidates_only.sort_values('overall_similarity', ascending=False)
    candidates_only['rank'] = range(1, len(candidates_only) + 1)
    
    # Cleanup temporary local columns
    candidates_only = candidates_only.drop(columns=local_pct_cols)
    
    return candidates_only

# Helper for the legend colors
def get_color(rank_bucket):
    colors = {
        "Top 10%": "#1a9850",   # Dark Green
        "10-25%": "#91cf60",    # Light Green
        "25-50%": "#fee08b",    # Yellow/Tan
        "50% +": "#d73027"      # Red
    }
    return colors.get(rank_bucket, "#gray")

# Add legend to the rendered map
def add_map_legend(m):
    legend_html = """
     <div style="
     position: fixed; 
     bottom: 50px; left: 50px; width: 150px; height: 160px; 
     background-color: white; border:2px solid grey; z-index:9999; font-size:14px;
     padding: 10px;
     border-radius: 5px;
     box-shadow: 2px 2px 5px rgba(0,0,0,0.3);
     ">
     <b>Market Rank</b><br>
     <i style="background: #00BFFF; width: 12px; height: 12px; float: left; margin-right: 5px; border: 1px solid black;"></i> Reference<br>
     <i style="background: #1a9850; width: 12px; height: 12px; float: left; margin-right: 5px; border: 1px solid black;"></i> Top 10%<br>
     <i style="background: #91cf60; width: 12px; height: 12px; float: left; margin-right: 5px; border: 1px solid black;"></i> 10-25%<br>
     <i style="background: #fee08b; width: 12px; height: 12px; float: left; margin-right: 5px; border: 1px solid black;"></i> 25-50%<br>
     <i style="background: #d73027; width: 12px; height: 12px; float: left; margin-right: 5px; border: 1px solid black;"></i> 50% +<br>
     </div>
     """
    m.get_root().html.add_child(folium.Element(legend_html))

# --- CONFIG & SESSION STATE ---
st.set_page_config(layout="wide", page_title="Site Similarity Hub")

# Initialize state to carry data between tabs
# Initialize list to save geoids for data lookup
if 'ref_geoids' not in st.session_state:
    st.session_state.ref_geoids = []

# Customer name  
if 'customer_name' not in st.session_state:
    st.session_state.customer_name = ""


# UI TABS ---
st.title("🌐 Market Similarity Discovery")
tab1, tab2, tab3 = st.tabs(["1. Reference", "2. Descriptive Stats", "3. Comparison"])

# --- TAB 1: REFERENCE ---
# --- TAB 1: REFERENCE ---
with tab1:
    st.header("Step 1: Define Target Profile")
    # Instead of this:
    # st.session_state.customer_name = st.text_input("Customer Name", value=st.session_state.customer_name)
    # Do this:
    # TODO: validate that this works 
    st.text_input("Customer Name", key="customer_name")
    
    selected_refs = []
    with st.container():
        # Get unique states as a Series to use .sort_values()
        unique_states = pd.Series(gdf['state_name'].unique()).sort_values()
        
        for i in range(1, 11):
            cols = st.columns([0.5, 2, 2])
            cols[0].write(f"#{i}")
            st_val = cols[1].selectbox(f"State", [""] + unique_states.tolist(), key=f"s{i}", label_visibility="collapsed")
            
            if st_val:
                # Filter and sort places using Pandas
                place_opts = gdf[gdf['state_name'] == st_val]['namelsad'].drop_duplicates().sort_values()
                pl_val = cols[2].selectbox(f"Place", [""] + place_opts.tolist(), key=f"p{i}", label_visibility="collapsed")
                
                if pl_val:
                    selected_refs.append((st_val, pl_val))
            else:
                cols[2].selectbox("Place", ["Select State"], disabled=True, key=f"p{i}", label_visibility="collapsed")

    if st.button("Generate Reference Profile"):
        if selected_refs:
            # Extract GEOIDs
            st.session_state.ref_geoids = [
                gdf[(gdf['state_name'] == s) & (gdf['namelsad'] == p)]['geoidfq'].iloc[0]
                for s, p in selected_refs
            ]
            st.success(f"Locked in {len(st.session_state.ref_geoids)} places. Move to Tab 2.")
        else:
            st.error("Please select at least one place.")

# --- TAB 2: DESCRIPTIVE STATS ---
with tab2:
    if not st.session_state.ref_geoids:
        st.info("Waiting for Reference Selection in Tab 1...")
    else:
        st.header(f"Reference Profile Benchmarks: {st.session_state.customer_name}")
        ref_df = gdf[gdf['geoidfq'].isin(st.session_state.ref_geoids)]
        
        summary_rows = []
        for group, metrics in METRIC_GROUPS.items():
            for m in metrics:
                # 1. Round off raw values (using .round(0))
                raw_vals = ref_df[m].astype(float)
                avg_pct = ref_df[f"{m}_pct"].mean()
                
                # 2. Logic for Tooltip (Min/Max identification)
                min_idx = raw_vals.idxmin()
                max_idx = raw_vals.idxmax()
                
                min_val = raw_vals.min()
                max_val = raw_vals.max()
                
                min_place = ref_df.loc[min_idx, 'namelsad']
                max_place = ref_df.loc[max_idx, 'namelsad']
                
                # We store the tooltip string to show in the dataframe
                hover_text = f"Min: {min_val:,.0f} ({min_place}) | Max: {max_val:,.0f} ({max_place})"
                
                summary_rows.append({
                    "Metric Group": group,
                    "Metric": m,
                    "Average (Raw)": round(raw_vals.mean(), 0),
                    "25th %ile": round(raw_vals.quantile(0.25), 0),
                    "Median": round(raw_vals.median(), 0),
                    "75th %ile": round(raw_vals.quantile(0.75), 0),
                    "Natl % Score": round(avg_pct, 1),
                    "Range Info": hover_text  # This will be used for the tooltip
                })
        
        summary_df = pd.DataFrame(summary_rows).set_index(["Metric Group", "Metric"])

        # 3. Apply Conditional Formatting (Heatmap)
        # We use 'RdYlGn' (Red-Yellow-Green) which is the standard Excel 'Traffic Light' scale
        styled_df = summary_df.style.background_gradient(
            subset=["Natl % Score"], 
            cmap="RdYlGn", 
            vmin=0, 
            vmax=100
        ).format({
            "Average (Raw)": "{:,.0f}",
            "25th %ile": "{:,.0f}",
            "Median": "{:,.0f}",
            "75th %ile": "{:,.0f}",
            "Natl % Score": "{:.1f}"
        })

        # Display the table with the 'Range Info' column used as help/tooltips
        # In Streamlit, we can use column_config to display the Range Info as a help tooltip
        st.dataframe(
            styled_df,
            column_config={
                "Range Info": st.column_config.TextColumn(
                    "Spread Details",
                    help="Hover over cells in this column to see the Min/Max contributors from your reference set."
                )
            },
            use_container_width=True
        )

# --- TAB 3: COMPARISON ---
# --- TAB 3: COMPARISON ---
# --- TAB 3: COMPARISON ---
with tab3:
    if not st.session_state.ref_geoids:
        st.info("Waiting for Reference Selection in Tab 1...")
    else:
        # SETUP STATE PERSISTENCE
        # This is needed for the map to persist and not vanish
        if 'analysis_results' not in st.session_state:
            st.session_state.analysis_results = None

        # SIDEBAR PARAMETERS
        with st.sidebar:
            st.header("Search Parameters")
            all_states = pd.Series(gdf['state_name'].unique()).sort_values()
            target_states = st.multiselect("Comparison States", all_states.tolist(), default=all_states.tolist())
            pop_min = st.number_input("Min Population", value=5000, step=1000)
            
            st.divider()
            st.header("Metric Group Weights")
            weights = {}
            for group in METRIC_GROUPS.keys():
                # Let's keep default weight = 1
                weights[group] = st.slider(f"{group}", 0.0, 1.0, 0.5)

            if st.button("🚀 Find Look-alike Markets", use_container_width=True):
                with st.spinner("Analyzing market similarity..."):
                    res = run_similarity_logic(gdf, st.session_state.ref_geoids, target_states, pop_min, weights)
                    if not res.empty:
                        total = len(res)
                        res['rank_bucket'] = res['rank'].apply(
                            lambda r: "Top 10%" if r <= total * 0.10 else 
                                      "10-25%" if r <= total * 0.25 else 
                                      "25-50%" if r <= total * 0.50 else "50% +"
                        )
                        st.session_state.analysis_results = res
                    else:
                        st.error("No matches found. Try adjusting population or state filters.")

        # 3. DISPLAY RESULTS
        if st.session_state.analysis_results is not None:
            results = st.session_state.analysis_results
            
            # --- MAP SECTION ---
            st.subheader(f"Strategic Market Map: {st.session_state.customer_name}")
            
            # Create Map
            # Zoom level 5 is usually the "sweet spot" for US-wide but focused views
            m = folium.Map(location=[39.8283, -98.5795], zoom_start=5, tiles='cartodbpositron')

            # A. PLOT REFERENCE CITIES (The Anchors)
            ref_gdf = gdf[gdf['geoidfq'].isin(st.session_state.ref_geoids)].copy()
            ref_gdf['geometry'] = ref_gdf.simplify(tolerance=0.01)
            
            for _, row in ref_gdf.iterrows():
                folium.GeoJson(
                    row['geometry'],
                    style_function=lambda x: {
                        'fillColor': '#00BFFF', # Deep Sky Blue
                        'color': '#00008B',     # Dark Blue Border
                        'weight': 3,            # Thicker border for references
                        'fillOpacity': 0.8,
                    },
                    tooltip=f"<b>REFERENCE:</b> {row['namelsad']}"
                ).add_to(m)

            # B. PLOT CANDIDATES (Top 500)
            map_gdf = results.head(500).copy()
            map_gdf['geometry'] = map_gdf.simplify(tolerance=0.01)

            for _, row in map_gdf.iterrows():
                # Build formatted tooltip
                tooltip_html = f"""
                <div style="font-family: sans-serif; font-size: 12px; min-width: 150px;">
                    <b style="font-size: 14px;">{row['namelsad']}, {row['stusps']}</b><br>
                    <span style="color: gray;">Rank: #{row['rank']}</span><br>
                    <hr style="margin: 5px 0;">
                    <b>Overall Sim: {row['overall_similarity']:.1f}%</b><br>
                    ACS: {row['acs_base_sim']:.1f}%<br>
                    Income: {row['acs_income_sim']:.1f}%<br>
                    Property: {row['property_sim']:.1f}%<br>
                    Growth: {row['growth_sim']:.1f}%
                </div>
                """
                
                folium.GeoJson(
                    row['geometry'],
                    style_function=lambda x, color=get_color(row['rank_bucket']): {
                        'fillColor': color,
                        'color': 'black',
                        'weight': 1.5, # Increased border value
                        'fillOpacity': 0.7,
                    },
                    tooltip=tooltip_html
                ).add_to(m)

            # Add legend
            add_map_legend(m)

            # Render the map
            st_folium(m, width=1400, height=650, key="discovery_map")

            st.divider()

            # --- TABLE SECTION ---
            st.subheader("Market Comparison Data (Top 50)")

            # Define group sim columns dynamically
            group_sim_cols = [f"{g.lower().replace(' ', '_')}_sim" for g in METRIC_GROUPS.keys()]
            display_cols = ['rank', 'namelsad', 'state_name', 'pop_2024', 'overall_similarity'] + group_sim_cols

            # Clean display selection
            final_table = results[display_cols].head(50)

            # Build formatter more explicitly to satisfy type checkers
            # Instead of using | to merge, we'll initialize and update
            formatter = {col: "{:.1f}%" for col in group_sim_cols}
            formatter['overall_similarity'] = "{:.1f}%"
            formatter['pop_2024'] = "{:,}"

            st.dataframe(
                final_table.style.format(formatter), # No changes needed to the call itself
                width="stretch",
                height=450
            )

            # CSV Export
            csv = results[display_cols].to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Full Search Results",
                data=csv,
                file_name=f"{st.session_state.customer_name}_market_discovery.csv",
                mime='text/csv',
            )