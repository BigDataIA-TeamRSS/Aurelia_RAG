import streamlit as st
import requests
import json
from datetime import datetime

# API configuration
API_BASE_URL = st.secrets.get("API_BASE_URL", "http://localhost:8000")

st.set_page_config(
    page_title="AURELIA - Financial Concept Notes",
    page_icon="📊",
    layout="wide"
)

st.title("📊 AURELIA - Automated Financial Concept Note Generator")
st.markdown("Generate comprehensive notes for financial concepts from the Financial Toolbox")

# Sidebar
with st.sidebar:
    st.header("Settings")
    force_regenerate = st.checkbox("Force Regenerate", help="Bypass cache and regenerate concept")
    show_citations = st.checkbox("Show Citations", value=True)
    
    st.markdown("---")
    st.markdown("### About")
    st.markdown("""
    AURELIA uses RAG (Retrieval-Augmented Generation) to create 
    structured concept notes from the Financial Toolbox PDF.
    
    Features:
    - 📄 PDF-first retrieval
    - 🌐 Wikipedia fallback
    - 💾 Intelligent caching
    - 📍 Source citations
    """)

# Main content
col1, col2 = st.columns([1, 2])

with col1:
    st.header("Query Concept")
    
    # Concept input
    concept = st.text_input(
        "Enter a financial concept:",
        placeholder="e.g., Sharpe Ratio, Black-Scholes, Duration"
    )
    
    # Common concepts for quick access
    st.markdown("### Quick Access")
    common_concepts = [
        "Sharpe Ratio", "Black-Scholes", "Duration",
        "Beta", "Alpha", "CAPM", "VaR", "Option Greeks"
    ]
    
    selected_concept = st.selectbox(
        "Or select a common concept:",
        [""] + common_concepts
    )
    
    if selected_concept:
        concept = selected_concept
    
    # Generate button
    generate_button = st.button("Generate Concept Note", type="primary", use_container_width=True)

with col2:
    st.header("Concept Note")
    
    if generate_button and concept:
        with st.spinner(f"Generating note for '{concept}'..."):
            try:
                # Make API request
                response = requests.post(
                    f"{API_BASE_URL}/query",
                    json={
                        "concept": concept,
                        "force_regenerate": force_regenerate
                    }
                )
                
                if response.status_code == 200:
                    note = response.json()
                    
                    # Display source indicator
                    source_color = "🟢" if note['source'] == "Financial Toolbox PDF" else "🔵"
                    st.markdown(f"{source_color} **Source:** {note['source']}")
                    
                    # Display concept note
                    st.markdown(f"## {note['concept']}")
                    
                    # Definition
                    st.markdown("### 📖 Definition")
                    st.markdown(note['definition'])
                    
                    # Key Points
                    st.markdown("### 🔑 Key Points")
                    for point in note['key_points']:
                        st.markdown(f"- {point}")
                    
                    # Formulas
                    if note.get('formulas'):
                        st.markdown("### 📐 Formulas")
                        for formula in note['formulas']:
                            st.latex(formula)
                    
                    # Examples
                    if note.get('examples'):
                        st.markdown("### 💡 Examples")
                        for example in note['examples']:
                            st.info(example)
                    
                    # Related Concepts
                    st.markdown("### 🔗 Related Concepts")
                    related_cols = st.columns(len(note['related_concepts']))
                    for idx, related in enumerate(note['related_concepts']):
                        with related_cols[idx]:
                            st.button(related, key=f"related_{idx}")
                    
                    # Citations
                    if show_citations and note.get('citations'):
                        with st.expander("📌 View Citations"):
                            for citation in note['citations']:
                                st.markdown(
                                    f"- Page {citation['page']}, "
                                    f"Section: {citation['section']} "
                                    f"(Relevance: {float(citation.get('score', 0)):.2%})"
                                )
                    
                    # Generation timestamp
                    st.caption(f"Generated at: {note.get('generated_at', 'N/A')}")
                    
                else:
                    st.error(f"Error: {response.status_code} - {response.text}")
                    
            except Exception as e:
                st.error(f"Failed to generate concept note: {str(e)}")
    
    elif not concept and generate_button:
        st.warning("Please enter a concept to generate a note.")
    
    else:
        # Welcome message
        st.info("""
        👈 Enter a financial concept to generate a comprehensive note.
        
        The system will:
        1. Search the Financial Toolbox PDF for relevant content
        2. Fall back to Wikipedia if no content is found
        3. Generate a structured note with citations
        """)

# Footer
st.markdown("---")
st.caption("AURELIA - Automated Financial Concept Note Generator | Cloud-Deployed RAG System")