# Post-Labor Economics: Triage Dashboard

An interactive visualization tool for the **Economic Agency Index (EAI)** that identifies US counties experiencing labor market stress and economic dependency shifts.

## 🎯 Purpose

This dashboard implements David Shapiro's Post-Labor Economics framework by mapping economic agency across all US counties. As automation and AI reshape the economy, traditional employment metrics miss the bigger picture. The Economic Agency Index reveals which communities are transitioning from wage dependency to transfer dependency—a critical early warning system for economic intervention.

**Key Insight:** Counties with declining wage ratios and rising transfer dependency need different solutions than traditional unemployment programs.

## 🚀 Quick Start

```bash
# 1. Clone and setup
git clone https://github.com/daveshap/PLE_Triage_Dashboard.git
cd PLE_Triage_Dashboard
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 2. Download and process data
python download_bea_data.py    # Downloads ~25MB from Bureau of Economic Analysis
python build_triage.py        # Processes 3,100+ counties into Economic Agency Index

# 3. Launch dashboard
streamlit run dashboard_triage.py
```

The dashboard will open in your browser showing all US counties color-coded by economic agency.

## 📊 What You'll See

- **3,114 US counties** visualized by Economic Agency Index
- **Interactive map** with counties color-coded by wage dependency levels
- **Real-time filtering** by economic stress thresholds
- **County rankings** showing areas requiring intervention
- **Data export** for further analysis

### Economic Agency Index Formula

```
EAI = Wage Income / (Wage Income + Property Income + Transfer Income)
```

- **Higher ratio (0.6-0.9):** More economic agency, self-sufficient communities
- **Medium ratio (0.35-0.6):** Balanced income mix, transitioning economies  
- **Lower ratio (0.1-0.35):** High transfer dependency, intervention needed

## 🔧 Technical Stack

- **Data Source:** Bureau of Economic Analysis (BEA) CAINC4 - Personal Income by County
- **Backend:** Python, DuckDB, pandas for data processing
- **Frontend:** Streamlit + PyDeck for interactive visualization
- **Update Frequency:** Annual (follows BEA release schedule)
- **Performance:** Optimized for 3,000+ county visualization

## 📈 Data Pipeline

1. **Extract:** Downloads latest BEA CAINC4 county personal income data
2. **Transform:** Calculates wage/property/transfer ratios for each county  
3. **Load:** Stores processed Economic Agency Index in DuckDB
4. **Visualize:** Interactive dashboard with geographic and trend analysis

### Data Components

- **LineCode 50:** Wage and salary income (economic agency)
- **LineCode 46:** Dividends, interest, and rent (property income)
- **LineCode 47:** Government transfers (dependency indicator)

## 🌍 Use Cases

### For Policymakers
- Identify counties transitioning from wage to transfer economies
- Target economic development resources to high-stress areas
- Monitor effectiveness of intervention programs over time

### For Researchers  
- Analyze geographic patterns of economic dependency
- Study correlation between automation and wage decline
- Export data for econometric modeling

### For Communities
- Understand local economic trajectory and positioning
- Benchmark against similar counties nationwide
- Advocate for appropriate economic development strategies

## 🎛️ Dashboard Features

### Interactive Controls
- **Year Selection:** View historical trends (2023 data available)
- **Stress Thresholds:** Customize what constitutes high/medium/low stress
- **County Filtering:** Focus on specific stress levels or regions
- **Data Export:** Download filtered datasets for offline analysis

### Visualization Layers
- **County Dots:** Color-coded by Economic Agency Index
- **Tooltips:** Detailed county information on hover
- **Legend:** Clear stress level indicators
- **Performance Optimized:** Smooth interaction with 3,000+ points

## 🔄 Updating Data

The dashboard uses the most recent BEA data (currently 2023). To update:

```bash
# Download latest data
python download_bea_data.py

# Rebuild database  
python build_triage.py

# Restart dashboard
streamlit run dashboard_triage.py
```

BEA typically releases county data with a 1-2 year lag, making this suitable for annual updates.

## 🎯 Post-Labor Economics Context

This dashboard operationalizes key concepts from David Shapiro's Post-Labor Economics framework:

- **Labor Collapse:** Systematic reduction in wage income share
- **Economic Agency:** Individual/community control over economic outcomes  
- **Transition Management:** Identifying areas needing policy intervention
- **Asset-Based Solutions:** Moving beyond traditional job creation to ownership models

### The "Better, Faster, Cheaper, Safer" Effect

As AI and automation make production systems better, faster, cheaper, and safer, they simultaneously reduce human labor demand. This dashboard reveals which counties are experiencing this transition first—and which policies might help communities adapt.

## 📜 Data

- **Data:** Bureau of Economic Analysis (public domain)
- **Generated Artifacts:** CC-BY 4.0
- **Methodology:** CC-BY-ND 4.0

## 🔗 Related Resources

- **Post-Labor Economics Research:** [David Shapiro's Substack](https://daveshap.io/)
- **BEA Data Documentation:** [Regional Economic Accounts](https://www.bea.gov/data/economic-accounts/regional)
- **Technical Methodology:** See `triage_spec.yaml` for detailed specifications

---

_**Built for Post-Labor Economics research by David Shapiro**  
*Identifying economic transitions before they become crises*_