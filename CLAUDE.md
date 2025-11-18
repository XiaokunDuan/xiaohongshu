# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## High-Level Code Architecture and Structure

This codebase implements a data analysis pipeline for "Xiaohongshu" (小红书) influencer data. The pipeline consists of three main Python scripts that perform sequential steps: data preprocessing, general data analysis, and then more specific analyses like value scoring, clustering, and regression.

### Data Flow

1.  `combined.csv` (Raw data)
2.  `process_data.py` -> `combined_cleaned_final.csv` (Preprocessed data)
3.  `analyze_data.py` -> `analysis_report.txt` (General analysis report)
4.  `1_value_score_analysis.py` (uses `combined_cleaned_final.csv`) -> `daren_value_scores.csv`, `daren_value_report.txt`
5.  `2_cluster_analysis.py` (uses `combined_cleaned_final.csv`) -> `daren_clusters.csv`, `cluster_analysis_report.txt`
6.  `3_regression_analysis.py` (uses `combined_cleaned_final.csv`) -> `daren_pricing_analysis.csv`, `regression_analysis_report.txt`, `correlation_heatmap.png`

### Key Components

*   **`process_data.py`**: Handles initial and detailed preprocessing of raw data from `combined.csv`. This includes cleaning, standardization, type conversions, and feature engineering.
*   **`analyze_data.py`**: Generates a comprehensive `analysis_report.txt` based on `combined_cleaned_final.csv`, covering overall influencer profiles, distributions, user persona analysis, content feature analysis, and interaction feature analysis.
*   **`1_value_score_analysis.py`**: Develops an influencer value assessment model, calculating various indices and a comprehensive value score. It identifies high-value influencers.
*   **`2_cluster_analysis.py`**: Segments influencers into distinct groups using K-Means clustering based on selected features.
*   **`3_regression_analysis.py`**: Performs correlation and regression analysis to identify factors influencing influencer pricing. It generates a correlation heatmap and builds a linear regression model.

### Key Libraries Used

*   `pandas`
*   `numpy`
*   `scikit-learn`
*   `matplotlib`
*   `seaborn`
*   `statsmodels`

### Installing Dependencies

If a `requirements.txt` file is not present, you can install the necessary libraries manually:
```bash
pip install pandas numpy scikit-learn matplotlib seaborn statsmodels
```

## Common Development Tasks

Since this is a script-based project, common development tasks involve running the individual Python scripts.

*   **Run Data Preprocessing**:
    ```bash
    python process_data.py
    ```
*   **Run General Data Analysis**:
    ```bash
    python analyze_data.py
    ```
*   **Run Value Score Analysis**:
    ```bash
    python 1_value_score_analysis.py
    ```
*   **Run Cluster Analysis**:
    ```bash
    python 2_cluster_analysis.py
    ```
*   **Run Regression Analysis**:
    ```bash
    python 3_regression_analysis.py
    ```
